#!/usr/bin/env python3
"""Fetch VM IPs from Prism and inject conntrack flows with 1:1, 1:N, or N:1 patterns."""

import argparse
import datetime
import logging
import os
import queue
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3
from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PAGE_SIZE = 50
LOG_FILE = "injected_conntrack_flows.log"
TCP_PORTS = [80, 443, 22, 3306]
UDP_PORTS = [53, 123, 161]
CONNTRACK_BIN = "/usr/sbin/conntrack"
# When an insert hits an existing tuple, update it if that entry is still active.
UPDATE_EXISTING_CONNECTION = True
ACTIVE_TCP_STATES = {"ESTABLISHED", "SYN_SENT", "SYN_RECV"}
VM_FILTER = "is_control_domain!=1;is_cvm==0"
SSH_TARGET = {"cvm_ip": None, "cvm_user": "nutanix", "cvm_password": None}

# Global teardown flag for graceful shutdown
_TEARDOWN_REQUESTED = threading.Event()
# One CVM login and one root@hypervisor shell per slot. Every conntrack
# command for that thread is written to the open shell. A new ssh per command
# makes the CVM refuse the extra channel.
CVM_SSH_SLOTS = 1
PRINT_LOCK = threading.Lock()
_HELD_SLOT = threading.local()
_CVM_SLOTS = queue.Queue()
_CVM_SLOTS_READY = False
_CVM_INIT_LOCK = threading.Lock()
_DIRECT_PROBE_LOCK = threading.Lock()
_DIRECT_PROBE = {"done": False, "use_cvm": False}

# Active schedule. Set this to one name from the blocks below.
#   sequential, same_vm, unique_vms, fan_out, fan_in, spread,
#   first_to_all, all_to_first
PARALLEL_MODE = "first_to_all"

# sequential — one flow finishes before the next starts
SEQUENTIAL_PARALLEL = False
SEQUENTIAL_THREAD_COUNT = 1

# same_vm — one source and one destination, several ports at once
SAME_VM_PARALLEL = True
SAME_VM_THREAD_COUNT = 5

# unique_vms — each running flow uses two VMs that no other flow is using 
UNIQUE_VMS_PARALLEL = True
UNIQUE_VMS_THREAD_COUNT = 5

# fan_out — one source sends to several destinations at once
FAN_OUT_PARALLEL = True
FAN_OUT_THREAD_COUNT = 50

# fan_in — several sources send to one destination at once, each on its own port
FAN_IN_PARALLEL = True
FAN_IN_THREAD_COUNT = 10

# spread — any flows at once; a VM may be in more than one flow
SPREAD_PARALLEL = True
SPREAD_THREAD_COUNT = 5

# first_to_all — first IP sends every common port to every other VM
FIRST_TO_ALL_PARALLEL = True
FIRST_TO_ALL_THREAD_COUNT = 50

# all_to_first — every other VM sends every common port to the first IP
ALL_TO_FIRST_PARALLEL = True
ALL_TO_FIRST_THREAD_COUNT = 50

# randomized_mesh — randomly selects source and destination pairs for chaos testing
RANDOMIZED_MESH_PARALLEL = True
RANDOMIZED_MESH_THREAD_COUNT = 50

# client_server_pair — splits VMs into two pools with one-way communication
CLIENT_SERVER_PAIR_PARALLEL = True
CLIENT_SERVER_PAIR_THREAD_COUNT = 50

# port_scan_burst — cycles a single source through port range to target
PORT_SCAN_BURST_PARALLEL = True
PORT_SCAN_BURST_THREAD_COUNT = 50

PARALLEL_OPTIONS = {
  "sequential": {
    "parallel": SEQUENTIAL_PARALLEL,
    "thread_count": SEQUENTIAL_THREAD_COUNT,
    "detail": "one flow at a time",
  },
  "same_vm": {
    "parallel": SAME_VM_PARALLEL,
    "thread_count": SAME_VM_THREAD_COUNT,
    "detail": "one source and one destination, several ports at once",
  },
  "unique_vms": {
    "parallel": UNIQUE_VMS_PARALLEL,
    "thread_count": UNIQUE_VMS_THREAD_COUNT,
    "detail": "each flow uses two VMs that no other flow is using",
  },
  "fan_out": {
    "parallel": FAN_OUT_PARALLEL,
    "thread_count": FAN_OUT_THREAD_COUNT,
    "detail": "one source sends to several destinations at once",
  },
  "fan_in": {
    "parallel": FAN_IN_PARALLEL,
    "thread_count": FAN_IN_THREAD_COUNT,
    "detail": "several sources send to one destination at once, each on its own port",
  },
  "spread": {
    "parallel": SPREAD_PARALLEL,
    "thread_count": SPREAD_THREAD_COUNT,
    "detail": "any flows at once; a VM may be in more than one flow",
  },
  "first_to_all": {
    "parallel": FIRST_TO_ALL_PARALLEL,
    "thread_count": FIRST_TO_ALL_THREAD_COUNT,
    "detail": "first IP sends every port to every other VM",
  },
  "all_to_first": {
    "parallel": ALL_TO_FIRST_PARALLEL,
    "thread_count": ALL_TO_FIRST_THREAD_COUNT,
    "detail": "every other VM sends every port to the first IP",
  },
  "randomized_mesh": {
    "parallel": RANDOMIZED_MESH_PARALLEL,
    "thread_count": RANDOMIZED_MESH_THREAD_COUNT,
    "detail": "randomly selects source and destination pairs for chaos testing",
  },
  "client_server_pair": {
    "parallel": CLIENT_SERVER_PAIR_PARALLEL,
    "thread_count": CLIENT_SERVER_PAIR_THREAD_COUNT,
    "detail": "splits VMs into two pools with one-way communication",
  },
  "port_scan_burst": {
    "parallel": PORT_SCAN_BURST_PARALLEL,
    "thread_count": PORT_SCAN_BURST_THREAD_COUNT,
    "detail": "cycles a single source through port range to target",
  },
}


def prism_session(pc_ip, username, password):
  session = requests.Session()
  session.auth = HTTPBasicAuth(username, password)
  session.verify = False
  return session


def fetch_hypervisors(pc_ip, username, password):
  """Return {host_uuid: hypervisorAddress} from GET /PrismGateway/services/rest/v1/hosts."""
  url = "https://{}:9440/PrismGateway/services/rest/v1/hosts".format(pc_ip)
  session = prism_session(pc_ip, username, password)
  resp = session.get(url, timeout=60)
  if resp.status_code != 200:
    raise RuntimeError(
      "hosts API failed status={} body={}".format(resp.status_code, resp.text[:500])
    )
  hosts = {}
  for entity in (resp.json().get("entities") or []):
    address = entity.get("hypervisorAddress")
    if address:
      hosts[entity.get("uuid")] = address
  if not hosts:
    raise RuntimeError("hosts API returned no hypervisorAddress")
  return hosts


def fetch_all_vm_ips(pc_ip, username, password, page_size=PAGE_SIZE, log=None):
  """Page through Prism Element GET /PrismGateway/services/rest/v1/vms.

  Also returns ip -> hypervisorAddress so conntrack runs on the host that owns the VM.
  """
  url = "https://{}:9440/PrismGateway/services/rest/v1/vms".format(pc_ip)
  session = prism_session(pc_ip, username, password)
  hypervisors = fetch_hypervisors(pc_ip, username, password)

  page = 1
  total_entities = None
  vm_ips = []
  ip_to_host = {}

  while True:
    resp = session.get(
      url,
      params={
        "count": page_size,
        "page": page,
        "sortCriteria": "-hypervisor_cpu_usage_ppm",
        "searchAttributeList": "vm_uuid",
        "projection": "basicInfo",
        "filterCriteria": VM_FILTER,
      },
      timeout=120,
    )
    if resp.status_code != 200:
      raise RuntimeError(
        "vms API failed at page={} status={} body={}".format(
          page, resp.status_code, resp.text[:500]
        )
      )

    data = resp.json()
    metadata = data.get("metadata") or {}
    if total_entities is None:
      total_entities = metadata.get("totalEntities")
      if total_entities is None:
        raise RuntimeError("totalEntities missing in vms response")
      log_and_print("totalEntities = {}".format(total_entities), log)

    entities = data.get("entities") or []
    if not entities:
      break

    for entity in entities:
      host = hypervisors.get(entity.get("hostUuid"))
      for ip in entity.get("ipAddresses") or []:
        if not ip or ip in vm_ips:
          if ip and host:
            ip_to_host.setdefault(ip, host)
          continue
        vm_ips.append(ip)
        if host:
          ip_to_host[ip] = host

    end_index = metadata.get("endIndex") or 0
    log_and_print(
      "fetched page {} endIndex {} / {} (unique IPs so far: {})".format(
        page, end_index, total_entities, len(vm_ips)
      ),
      log,
    )
    if end_index >= total_entities or len(entities) < page_size:
      break
    page += 1

  return vm_ips, ip_to_host


def log_and_print(message, file_handle=None):
  with PRINT_LOCK:
    print(message, flush=True)
    if file_handle is not None:
      file_handle.write(message + "\n")
      file_handle.flush()


def _signal_handler_teardown(signum, frame):
  """Handle SIGUSR1 for graceful teardown with flow flush."""
  global _TEARDOWN_REQUESTED
  log_and_print("\n[SIGNAL] Received signal {} - initiating graceful teardown".format(signum))
  _TEARDOWN_REQUESTED.set()


def flush_injected_flows(flows, log_file_handle=None):
  """Delete all mock flows from conntrack."""
  log_and_print("Flushing {} injected mock flows...".format(len(flows)), log_file_handle)
  flushed = 0
  failed = 0
  
  for flow in flows:
    host = flow.get("_host")
    if not host:
      continue
    
    try:
      delete_cmd = [
        CONNTRACK_BIN, "-D",
        "-p", flow["proto"],
        "--orig-src", flow["orig_src"],
        "--orig-dst", flow["orig_dst"],
        "--sport", flow["sport"],
        "--dport", flow["dport"],
      ]
      res = run_on_hypervisor(host, delete_cmd)
      if res.returncode == 0:
        flushed += 1
      else:
        failed += 1
        log_and_print("  [FLUSH WARN] {}:{} -> {}:{} delete failed: {}".format(
          flow["orig_src"], flow["sport"], flow["orig_dst"], flow["dport"],
          (res.stderr or res.stdout or "").strip()[:100]
        ), log_file_handle)
    except Exception as exc:
      failed += 1
      log_and_print("  [FLUSH ERROR] {}:{} -> {}:{} exception: {}".format(
        flow["orig_src"], flow["sport"], flow["orig_dst"], flow["dport"], exc
      ), log_file_handle)
  
  log_and_print("Flushed {}/{} flows ({} failed)".format(flushed, len(flows), failed), log_file_handle)
  return flushed, failed


def make_flow(src, dst, proto, sport, dport, timeout, tcp_state, status):
  flow = {
    "proto": proto,
    "orig_src": src,
    "orig_dst": dst,
    "sport": str(sport),
    "dport": str(dport),
    "reply_src": dst,
    "reply_dst": src,
    "reply_sport": str(dport),
    "reply_dport": str(sport),
    "timeout": str(timeout),
  }
  if proto == "tcp":
    flow["state"] = tcp_state
  if status:
    flow["status"] = status
  return flow


def _parse_port_list(raw):
  """Return ports from commas, spaces, or start-end ranges. Empty or 0 means none."""
  text = "" if raw is None else str(raw).strip()
  if text in ("", "0"):
    return []
  ports = []
  seen = set()
  limit = 1024
  for token in text.replace(",", " ").split():
    if "-" in token:
      ends = token.split("-")
      if len(ends) != 2:
        sys.exit("Port range %r is invalid" % token)
      try:
        start = int(ends[0])
        end = int(ends[1])
      except ValueError:
        sys.exit("Port range %r is invalid" % token)
      if not 1 <= start <= end <= 65535:
        sys.exit("Port range %s must stay inside 1-65535" % token)
      width = end - start + 1
      if len(ports) + width > limit:
        sys.exit("Port list is larger than %s ports" % limit)
      for port in range(start, end + 1):
        if port not in seen:
          seen.add(port)
          ports.append(port)
      continue
    try:
      port = int(token)
    except ValueError:
      sys.exit("Port %r is not an integer" % token)
    if not 1 <= port <= 65535:
      sys.exit("Port %s is outside 1-65535" % port)
    if len(ports) >= limit:
      sys.exit("Port list is larger than %s ports" % limit)
    if port not in seen:
      seen.add(port)
      ports.append(port)
  return ports


def _ports_for_proto(proto, dport, dports, tcp_ports, udp_ports):
  """Chosen ports for one protocol. An empty choice uses that protocol's common ports."""
  if tcp_ports or udp_ports:
    chosen = list(tcp_ports or []) if proto == "tcp" else list(udp_ports or [])
  else:
    chosen = list(dports or [])
  if not chosen and dport:
    chosen = [dport]
  if chosen:
    return chosen
  return list(TCP_PORTS if proto == "tcp" else UDP_PORTS)


def build_flows(vm_ips, pattern, n, proto, sport, dport, timeout, tcp_state, status, dports=None, tcp_ports=None, udp_ports=None, sports=None):
  if len(vm_ips) < 2:
    raise RuntimeError("need at least 2 unique VM IPs to inject flows")

  n = max(1, min(n, len(vm_ips) - 1))
  protos = ["tcp", "udp"] if proto == "both" else [proto]
  pairs = []
  if pattern == "1:1":
    for i in range(len(vm_ips) // 2):
      pairs.append((vm_ips[i * 2], vm_ips[i * 2 + 1]))
  elif pattern == "1:n":
    for dst in vm_ips[1:1 + n]:
      pairs.append((vm_ips[0], dst))
  elif pattern == "n:1":
    for src in vm_ips[1:1 + n]:
      pairs.append((src, vm_ips[0]))
  elif pattern == "1:all":
    for dst in vm_ips[1:]:
      pairs.append((vm_ips[0], dst))
  elif pattern == "all:1":
    for src in vm_ips[1:]:
      pairs.append((src, vm_ips[0]))
  else:
    raise RuntimeError("unknown pattern {}".format(pattern))

  flows = []
  sports = list(sports or [])
  for one_proto in protos:
    ports = _ports_for_proto(one_proto, dport, dports, tcp_ports, udp_ports)
    for src, dst in pairs:
      for port in ports:
        if sports:
          use_sport = sports[len(flows) % len(sports)]
        else:
          use_sport = sport if sport else 40000 + (len(flows) % 20000)
        flows.append(make_flow(src, dst, one_proto, use_sport, port, timeout, tcp_state, status))
  return flows


def _group_in_order(jobs, keyfn):
  order = []
  buckets = {}
  for job in jobs:
    key = keyfn(job)
    if key not in buckets:
      buckets[key] = []
      order.append(key)
    buckets[key].append(job)
  return [buckets[key] for key in order]


def _interleave(jobs, keyfn):
  groups = _group_in_order(jobs, keyfn)
  mixed = []
  pending = True
  while pending:
    pending = False
    for group in groups:
      if group:
        mixed.append(group.pop(0))
        pending = True
  return mixed


def _chunk(items, size):
  return [items[index:index + size] for index in range(0, len(items), size)]


def _select_waves(jobs, width, join_keys):
  remaining = list(jobs)
  waves = []
  while remaining:
    used = set()
    wave = []
    deferred = []
    for job in remaining:
      if len(wave) >= width:
        deferred.append(job)
        continue
      keys = join_keys(job, used)
      if keys is None:
        deferred.append(job)
        continue
      wave.append(job)
      used.update(keys)
    if not wave:
      wave.append(deferred.pop(0))
    waves.append(wave)
    remaining = deferred
  return waves


def build_waves(mode, flows, width):
  """Group conntrack inserts the same way vm_ipfix_traffic groups live flows."""
  if mode == "sequential" or width <= 1:
    return [[flow] for flow in flows]
  if mode == "same_vm":
    waves = []
    for group in _group_in_order(flows, lambda flow: (flow["orig_src"], flow["orig_dst"])):
      waves.extend(_chunk(group, width))
    return waves
  if mode == "unique_vms":
    def unique_vm_keys(flow, used):
      src_ip = flow["orig_src"]
      dst_ip = flow["orig_dst"]
      if src_ip in used or dst_ip in used:
        return None
      return (src_ip, dst_ip)
    return _select_waves(flows, width, unique_vm_keys)
  if mode == "fan_out":
    waves = []
    for group in _group_in_order(flows, lambda flow: flow["orig_src"]):
      mixed = _interleave(group, lambda flow: flow["orig_dst"])

      def fan_out_keys(flow, used):
        dst_ip = flow["orig_dst"]
        if dst_ip in used:
          return None
        return (dst_ip,)
      waves.extend(_select_waves(mixed, width, fan_out_keys))
    return waves
  if mode == "fan_in":
    waves = []
    for group in _group_in_order(flows, lambda flow: flow["orig_dst"]):
      mixed = _interleave(group, lambda flow: flow["orig_src"])

      def fan_in_keys(flow, used):
        src_ip = flow["orig_src"]
        port_key = ("port", flow["proto"], flow["dport"])
        if src_ip in used or port_key in used:
          return None
        return (src_ip, port_key)
      waves.extend(_select_waves(mixed, width, fan_in_keys))
    return waves
  if mode == "spread":
    return [_interleave(flows, lambda flow: (flow["orig_src"], flow["orig_dst"]))]
  if mode == "first_to_all":
    mixed = _interleave(flows, lambda flow: flow["orig_dst"])

    def first_to_all_keys(flow, used):
      dst_ip = flow["orig_dst"]
      if dst_ip in used:
        return None
      return (dst_ip,)
    return _select_waves(mixed, width, first_to_all_keys)
  if mode == "all_to_first":
    mixed = _interleave(flows, lambda flow: flow["orig_src"])

    def all_to_first_keys(flow, used):
      src_ip = flow["orig_src"]
      if src_ip in used:
        return None
      return (src_ip,)
    return _select_waves(mixed, width, all_to_first_keys)
  if mode == "randomized_mesh":
    # Shuffle flows to create random source-destination pairs
    import random
    shuffled = flows[:]
    random.shuffle(shuffled)
    return _chunk(shuffled, width)
  if mode == "client_server_pair":
    # Split VMs into two pools: first half as clients, second half as servers
    # Only allow flows from first pool to second pool
    if not flows:
      return []
    all_vms = set()
    for flow in flows:
      all_vms.add(flow["orig_src"])
      all_vms.add(flow["orig_dst"])
    vm_list = sorted(all_vms)
    midpoint = len(vm_list) // 2
    if midpoint == 0:
      midpoint = 1
    clients = set(vm_list[:midpoint])
    servers = set(vm_list[midpoint:])
    
    # Filter flows to only include client -> server communication
    filtered = [
      flow for flow in flows
      if flow["orig_src"] in clients and flow["orig_dst"] in servers
    ]
    if not filtered:
      # Fallback: if no valid flows, use original flows
      filtered = flows
    return _chunk(filtered, width)
  if mode == "port_scan_burst":
    # Single source cycling through ports to target(s)
    # Group by source-destination pair, interleave by port
    waves = []
    for group in _group_in_order(flows, lambda flow: (flow["orig_src"], flow["orig_dst"])):
      # Interleave by destination port to spread port scan
      mixed = _interleave(group, lambda flow: flow["dport"])
      waves.extend(_chunk(mixed, width))
    return waves
  raise RuntimeError("unknown parallel mode {}".format(mode))


def _ssh_direct(host, remote):
  direct = [
    "ssh",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=8",
    "root@{}".format(host),
    remote,
  ]
  return subprocess.run(direct, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _direct_needs_cvm(res):
  text = (res.stderr or "") + (res.stdout or "")
  return (
    "Connection refused" in text
    or "No route to host" in text
    or "conntrack: command not found" in text
    or "No such file or directory" in text
  )


def _annotate_direct(res, host):
  if res.returncode != 0 and "Permission denied" in (res.stderr or ""):
    res.stderr = "hypervisor SSH rejected for root@{}: {}".format(host, res.stderr.strip())
  return res


def run_on_hypervisor(host, args):
  """Run conntrack as root on the AHV host.

  From this machine the hypervisor SSH port is closed, so the command is sent
  through the CVM: ssh nutanix@<cvm> "ssh root@<host> 'conntrack ...'".
  If this process is already on the CVM, ssh root@<host> is used directly.
  The direct probe runs once. Later flows reuse that choice and a small set
  of CVM sessions instead of opening one SSH login per flow.
  """
  remote = " ".join(shlex.quote(part) for part in args)
  with _DIRECT_PROBE_LOCK:
    if not _DIRECT_PROBE["done"]:
      res = _ssh_direct(host, remote)
      needs_cvm = _direct_needs_cvm(res)
      _DIRECT_PROBE["done"] = True
      _DIRECT_PROBE["use_cvm"] = needs_cvm
      if not needs_cvm:
        return _annotate_direct(res, host)
    use_cvm = _DIRECT_PROBE["use_cvm"]
  if use_cvm:
    return _run_on_root_shell(host, args)
  return _annotate_direct(_ssh_direct(host, remote), host)


def _cvm_alive(client):
  if client is None:
    return False
  transport = client.get_transport()
  return transport is not None and transport.is_active()


def _connect_cvm(paramiko_mod):
  """Open one CVM session, retrying when sshd resets the banner."""
  target = SSH_TARGET
  login = "{}@{}".format(target["cvm_user"], target["cvm_ip"])
  last = None
  for attempt in range(3):
    client = paramiko_mod.SSHClient()
    client.set_missing_host_key_policy(paramiko_mod.AutoAddPolicy())
    try:
      client.connect(
        target["cvm_ip"],
        username=target["cvm_user"],
        password=target["cvm_password"],
        timeout=20,
        banner_timeout=30,
        auth_timeout=20,
      )
      return client, None
    except paramiko_mod.AuthenticationException as exc:
      client.close()
      detail = "CVM SSH password rejected for {} (root@hypervisor was not tried): {}".format(login, exc)
      target["error"] = detail
      return None, detail
    except Exception as exc:
      try:
        client.close()
      except Exception:
        pass
      last = exc
      time.sleep(0.5 * (attempt + 1))
  return None, "CVM SSH to {} failed: {}".format(login, last)


class _RootShell(object):
  """One nutanix@CVM session with one root@hypervisor shell kept open."""

  def __init__(self):
    self.client = None
    self.channel = None
    self.stdin = None
    self.stdout = None
    self.stderr = None
    self.host = None
    self.buf = ""
    self.seq = 0

  def close(self):
    for item in (self.stdin, self.channel, self.client):
      if item is None:
        continue
      try:
        item.close()
      except Exception:
        pass
    self.client = None
    self.channel = None
    self.stdin = None
    self.stdout = None
    self.stderr = None
    self.host = None
    self.buf = ""

  def healthy(self, host):
    if self.host != host or self.client is None or self.channel is None or self.stdin is None:
      return False
    if self.channel.closed or not _cvm_alive(self.client):
      return False
    return True

  def ensure(self, host, paramiko_mod):
    if self.healthy(host):
      return None
    self.close()
    client, err = _connect_cvm(paramiko_mod)
    if client is None:
      return err
    login = "{}@{}".format(SSH_TARGET["cvm_user"], SSH_TARGET["cvm_ip"])
    inner = (
      "ssh -T -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
      "-o LogLevel=ERROR -o BatchMode=yes root@{} sh -s"
    ).format(host)
    try:
      stdin, stdout, stderr = client.exec_command(inner, timeout=30)
      self.client = client
      self.stdin = stdin
      self.stdout = stdout
      self.stderr = stderr
      self.channel = stdout.channel
      self.host = host
      self.buf = ""
      self._run_line("true", 20)
    except Exception as exc:
      self.close()
      try:
        client.close()
      except Exception:
        pass
      return "hypervisor SSH rejected for root@{} after CVM login {}: {}".format(login, host, exc)
    return None

  def run_argv(self, args, timeout=45):
    remote = " ".join(shlex.quote(part) for part in args)
    return self._run_line(remote, timeout)

  def _run_line(self, remote, timeout):
    self.seq += 1
    marker = "__CT_END_%s__" % self.seq
    script = "{ %s ; } 2>&1\nprintf '\\n%%s %%s\\n' %s $?\n" % (remote, shlex.quote(marker))
    self.stdin.write(script)
    self.stdin.flush()
    status_line, lines = self._read_until(marker, timeout)
    parts = status_line.split()
    try:
      code = int(parts[-1])
    except (IndexError, ValueError):
      code = 1
    return code, "\n".join(lines).strip()

  def _read_until(self, marker, timeout):
    deadline = time.time() + timeout
    kept = []
    while time.time() < deadline:
      if self.channel.recv_ready():
        self.buf += self.channel.recv(8192).decode("utf-8", "replace")
      if self.channel.recv_stderr_ready():
        self.buf += self.channel.recv_stderr(8192).decode("utf-8", "replace")
      if "\n" in self.buf:
        lines = self.buf.split("\n")
        for index, line in enumerate(lines[:-1]):
          if line.startswith(marker):
            self.buf = "\n".join(lines[index + 1:])
            return line, kept
          if line.strip() and "Permanently added" not in line:
            kept.append(line)
        self.buf = lines[-1]
      if self.channel.exit_status_ready() and not self.channel.recv_ready() and not self.channel.recv_stderr_ready():
        tail = self.buf.strip()
        detail = "\n".join(kept + ([tail] if tail else [])) or "root shell closed"
        self.buf = ""
        raise RuntimeError(detail)
      time.sleep(0.02)
    raise TimeoutError("timed out waiting for conntrack on root@%s" % self.host)


def _ensure_cvm_slots():
  global _CVM_SLOTS_READY
  with _CVM_INIT_LOCK:
    if _CVM_SLOTS_READY:
      return
    for _ in range(max(1, CVM_SSH_SLOTS)):
      _CVM_SLOTS.put(_RootShell())
    _CVM_SLOTS_READY = True


def _borrow_cvm_slot():
  _ensure_cvm_slots()
  return _CVM_SLOTS.get()


def _return_cvm_slot(slot):
  _CVM_SLOTS.put(slot)


def _exec_on_slot(slot, host, args, paramiko_mod):
  """Run one conntrack argv on the slot's open root shell, retrying a dropped session."""
  target = SSH_TARGET
  login = "{}@{}".format(target["cvm_user"], target["cvm_ip"])
  last = None
  for attempt in range(3):
    if target.get("error"):
      return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=target["error"])
    err = slot.ensure(host, paramiko_mod)
    if err:
      last = err
      if target.get("error") or "password rejected" in err:
        break
      slot.close()
      time.sleep(0.4 * (attempt + 1))
      continue
    try:
      code, text = slot.run_argv(args)
    except Exception as exc:
      last = "CVM SSH to {} failed before root@{}: {}".format(login, host, exc)
      slot.close()
      time.sleep(0.4 * (attempt + 1))
      continue
    stderr = "" if code == 0 else text
    return subprocess.CompletedProcess(args=args, returncode=code, stdout=text, stderr=stderr)
  return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=last or "CVM SSH failed")


def _run_on_root_shell(host, args):
  """Use this thread's open root shell when it has one, so every command stays there."""
  try:
    import paramiko
  except ImportError:
    raise RuntimeError("paramiko is required to reach the hypervisor through the CVM")
  logging.getLogger("paramiko").setLevel(logging.CRITICAL)
  if SSH_TARGET.get("error"):
    return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=SSH_TARGET["error"])
  slot = getattr(_HELD_SLOT, "slot", None)
  own = slot is None
  if own:
    slot = _borrow_cvm_slot()
  try:
    return _exec_on_slot(slot, host, args, paramiko)
  finally:
    if own:
      _return_cvm_slot(slot)


def inject_argv(flow):
  cmd = [
    CONNTRACK_BIN, "-I",
    "-p", flow["proto"],
    "--orig-src", flow["orig_src"],
    "--orig-dst", flow["orig_dst"],
    "--sport", flow["sport"],
    "--dport", flow["dport"],
    "--reply-src", flow["reply_src"],
    "--reply-dst", flow["reply_dst"],
    "--reply-port-src", flow["reply_sport"],
    "--reply-port-dst", flow["reply_dport"],
    "--timeout", flow["timeout"],
  ]
  if flow["proto"] == "tcp":
    cmd.extend(["--state", flow["state"]])
  if "status" in flow:
    cmd.extend(["--status", flow["status"]])
  return cmd


def update_argv(flow):
  cmd = inject_argv(flow)
  cmd[1] = "-U"
  return cmd


def lookup_existing_state(flow, host):
  """Return the TCP state of an existing tuple, or ACTIVE for a live UDP entry."""
  res = run_on_hypervisor(host, [
    CONNTRACK_BIN, "-L",
    "-p", flow["proto"],
    "--orig-src", flow["orig_src"],
    "--orig-dst", flow["orig_dst"],
    "--sport", flow["sport"],
    "--dport", flow["dport"],
  ])
  text = ((res.stdout or "") + "\n" + (res.stderr or "")).splitlines()
  for line in text:
    if "src=%s" % flow["orig_src"] not in line or "dport=%s" % flow["dport"] not in line:
      continue
    state = re.search(r"\s([A-Z][A-Z0-9_]+)\s+src=", line)
    if state:
      return state.group(1)
    return "ACTIVE"
  return None


def connection_is_active(proto, state):
  if not state:
    return False
  if proto == "udp":
    return True
  return state in ACTIVE_TCP_STATES


def inject_flow(flow, host):
  """Inject one flow. An existing active tuple is updated when UPDATE_EXISTING_CONNECTION is set."""
  res = run_on_hypervisor(host, inject_argv(flow))
  detail = (res.stderr or res.stdout).strip()
  if res.returncode == 0:
    return True, detail
  if "Such conntrack exists" not in detail or not UPDATE_EXISTING_CONNECTION:
    return False, detail
  state = lookup_existing_state(flow, host)
  if not connection_is_active(flow["proto"], state):
    return False, "existing conntrack state=%s is not active; left unchanged" % (state or "missing")
  updated = run_on_hypervisor(host, update_argv(flow))
  update_detail = (updated.stderr or updated.stdout).strip()
  if updated.returncode == 0:
    return True, "updated existing %s conntrack" % state
  return False, update_detail


def inspect_target_flows(target_flows, host, log_file_handle):
  res = run_on_hypervisor(host, [CONNTRACK_BIN, "-L", "-o", "extended"])

  if res.returncode != 0:
    log_and_print(
      "[{}] ERROR querying conntrack: {}".format(datetime.datetime.now(), res.stderr.strip()),
      log_file_handle,
    )
    return

  lines = res.stdout.strip().split("\n") if res.stdout.strip() else []
  pairs = {(flow["orig_src"], flow["orig_dst"]) for flow in target_flows}

  header = "{:<10} {:<25} {:<25} {:<10} {:<10} {}".format(
    "PROTOCOL", "SRC IP:PORT", "DST IP:PORT", "PACKETS", "BYTES", "TYPE"
  )
  separator = "=" * 88
  log_and_print(header, log_file_handle)
  log_and_print(separator, log_file_handle)

  found_count = 0
  for line in lines:
    src_match = re.search(r"src=([\d\.]+)", line)
    dst_match = re.search(r"dst=([\d\.]+)", line)
    if not src_match or not dst_match:
      continue
    if (src_match.group(1), dst_match.group(1)) not in pairs:
      continue

    found_count += 1
    proto_match = re.search(r"^(\w+)\s+", line)
    sport_match = re.search(r"sport=(\d+)", line)
    dport_match = re.search(r"dport=(\d+)", line)
    pkt_match = re.search(r"packets=(\d+)", line)
    byte_match = re.search(r"bytes=(\d+)", line)

    proto = proto_match.group(1).upper() if proto_match else "N/A"
    src = "{}:{}".format(src_match.group(1), sport_match.group(1) if sport_match else "?")
    dst = "{}:{}".format(dst_match.group(1), dport_match.group(1) if dport_match else "?")
    pkts = int(pkt_match.group(1)) if pkt_match else 0
    bytes_cnt = int(byte_match.group(1)) if byte_match else 0
    flow_type = "Mock (0 bytes)" if pkts == 0 and bytes_cnt == 0 else "Real Traffic"
    log_and_print(
      "{:<10} {:<25} {:<25} {:<10} {:<10} {}".format(proto, src, dst, pkts, bytes_cnt, flow_type),
      log_file_handle,
    )

  if found_count == 0:
    log_and_print("No matching injected flows found in active conntrack table.", log_file_handle)
  else:
    log_and_print("Matched {} conntrack entries.".format(found_count), log_file_handle)


def main():
  global UPDATE_EXISTING_CONNECTION
  parser = argparse.ArgumentParser(
    description="Fetch VM IPs from Prism and inject conntrack flows"
  )
  parser.add_argument("--pc_ip", default="10.120.132.47", help="Prism Element IP")
  parser.add_argument("--username", default="admin", help="Prism username")
  parser.add_argument("--password", default="Nutanix.123", help="Prism password")
  parser.add_argument("--page_size", type=int, default=PAGE_SIZE, help="vms API page size")
  parser.add_argument(
    "--ips_file",
    default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ips_on_host.txt"),
    help="Write/read space-separated VM IPs",
  )
  parser.add_argument(
    "--skip_fetch",
    action="store_true",
    help="Use --ips_file instead of calling the Prism vms API",
  )
  parser.add_argument(
    "--pattern",
    choices=["1:1", "1:n", "n:1", "1:all", "all:1"],
    default="1:1",
    help="Flow pattern: 1vm:1vm (default), 1vm:n vm, n vms:1 vm, first IP to every VM, every VM to the first IP",
  )
  parser.add_argument(
    "--n",
    type=int,
    default=10,
    help="N for 1:n and n:1 patterns (default 10)",
  )
  parser.add_argument("--proto", choices=["tcp", "udp", "both"], default="tcp")
  parser.add_argument("--sport", type=int, default=0, help="Fixed source port; 0 auto-assigns unless --sports is set")
  parser.add_argument("--sports", default="", help="Source ports as commas or a start-end range. Flows walk this list.")
  parser.add_argument("--dport", type=int, default=0, help="One destination port. Ignored when --dports or protocol port lists are set.")
  parser.add_argument("--dports", default="", help="Destination ports as commas or a start-end range. Empty uses the common ports.")
  parser.add_argument("--tcp-ports", default="", help="TCP destination ports when --proto is both.")
  parser.add_argument("--udp-ports", default="", help="UDP destination ports when --proto is both.")
  parser.add_argument("--timeout", type=int, default=120)
  parser.add_argument("--tcp_state", default="ESTABLISHED")
  parser.add_argument("--status", default="SEEN_REPLY,ASSURED", help="Status flags to add to flows")
  parser.add_argument("--parallel-mode", default=PARALLEL_MODE, choices=tuple(PARALLEL_OPTIONS),
                      help="How injected flows share VMs. Default: %s" % PARALLEL_MODE)
  parser.add_argument("--parallel", action=argparse.BooleanOptionalAction, default=None,
                      help="Override the selected mode's *_PARALLEL flag")
  parser.add_argument("--thread-count", type=int, default=None,
                      help="Override the selected mode's *_THREAD_COUNT")
  parser.add_argument("--host_ip", default=None, help="AHV host to ssh as root for conntrack. Default: hypervisorAddress from the hosts API")
  parser.add_argument("--cvm_user", default="nutanix", help="CVM user for ssh root@hypervisor when the host is not reachable directly")
  parser.add_argument("--cvm_password", default="RDMCluster.123", help="CVM SSH password. Defaults to --password")
  parser.add_argument("--update-existing", action=argparse.BooleanOptionalAction, default=None,
                      help="Update an existing active conntrack entry instead of failing. Default: %s" % UPDATE_EXISTING_CONNECTION)
  parser.add_argument("--inspect_only", action="store_true", help="Skip inject; only inspect")
  parser.add_argument("--keep-alive", action="store_true", help="Enable keep-alive mode: periodically refresh flows to prevent timeout")
  parser.add_argument("--keepalive-interval", type=int, default=60, help="Keep-alive refresh interval in seconds (default: 60)")
  parser.add_argument("--duration-limit", type=int, default=None, help="Maximum duration for keep-alive in seconds before auto-teardown")
  parser.add_argument("--flush-mock", action="store_true", help="Flush (delete) all injected mock flows on exit")
  args = parser.parse_args()
  if args.parallel_mode not in PARALLEL_OPTIONS:
    sys.exit("Unknown --parallel-mode %r" % args.parallel_mode)
  option = PARALLEL_OPTIONS[args.parallel_mode]
  parallel = option["parallel"] if args.parallel is None else args.parallel
  mode = args.parallel_mode if parallel else "sequential"
  thread_count = option["thread_count"] if args.thread_count is None else args.thread_count
  if not parallel:
    thread_count = 1
  if thread_count < 1:
    sys.exit("--thread-count must be at least 1")
  if args.update_existing is not None:
    UPDATE_EXISTING_CONNECTION = args.update_existing
  SSH_TARGET["cvm_ip"] = args.pc_ip
  SSH_TARGET["cvm_user"] = args.cvm_user
  SSH_TARGET["cvm_password"] = args.cvm_password or args.password

  # Register signal handler for graceful teardown
  if args.keep_alive or args.flush_mock:
    signal.signal(signal.SIGUSR1, _signal_handler_teardown)
    log_and_print("[INIT] Registered SIGUSR1 handler for graceful teardown")

  with open(LOG_FILE, "a") as log:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_and_print("\n--- INGEST & INSPECT RUN: {} ---".format(timestamp), log)
    if args.parallel_mode == "first_to_all":
      pattern = "1:all"
    elif args.parallel_mode == "all_to_first":
      pattern = "all:1"
    else:
      pattern = args.pattern
    log_and_print("pattern={} n={} proto={} mode={} update_existing={}".format(
      pattern, args.n, args.proto, mode, UPDATE_EXISTING_CONNECTION), log)

    ip_to_host = {}
    if args.skip_fetch:
      if not os.path.exists(args.ips_file):
        log_and_print("ERROR: ips file not found: {}".format(args.ips_file), log)
        sys.exit(1)
      with open(args.ips_file) as fh:
        vm_ips = fh.read().split()
      log_and_print("Loaded {} IPs from {}".format(len(vm_ips), args.ips_file), log)
    else:
      try:
        vm_ips, ip_to_host = fetch_all_vm_ips(
          args.pc_ip, args.username, args.password, args.page_size, log
        )
      except Exception as exc:
        log_and_print("ERROR fetching VM IPs: {}".format(exc), log)
        sys.exit(1)
      with open(args.ips_file, "w") as fh:
        fh.write(" ".join(vm_ips) + "\n")
      log_and_print("Wrote {} IPs to {}".format(len(vm_ips), args.ips_file), log)

    if args.host_ip:
      default_hosts = [args.host_ip]
    else:
      try:
        default_hosts = list(dict.fromkeys(fetch_hypervisors(
          args.pc_ip, args.username, args.password
        ).values()))
      except Exception as exc:
        log_and_print("ERROR fetching hypervisor address: {}".format(exc), log)
        sys.exit(1)
    log_and_print("conntrack via ssh root@{}".format(",".join(default_hosts)), log)
    log_and_print(
      "CVM SSH sessions=%s; each keeps one root@hypervisor shell and runs that thread's commands there"
      % max(1, CVM_SSH_SLOTS),
      log,
    )

    def host_for(ip):
      if args.host_ip:
        return args.host_ip
      if ip in ip_to_host:
        return ip_to_host[ip]
      if len(default_hosts) == 1:
        return default_hosts[0]
      raise RuntimeError("no hypervisor for VM {}".format(ip))

    try:
      target_flows = build_flows(
        vm_ips, pattern, args.n, args.proto,
        args.sport or None, args.dport or None, args.timeout, args.tcp_state,
        args.status,
        dports=_parse_port_list(args.dports),
        tcp_ports=_parse_port_list(args.tcp_ports),
        udp_ports=_parse_port_list(args.udp_ports),
        sports=_parse_port_list(args.sports),
    )
    except Exception as exc:
      log_and_print("ERROR building flows: {}".format(exc), log)
      sys.exit(1)

    log_and_print("Built {} flows for pattern {}".format(len(target_flows), pattern), log)
    waves = []
    for wave in build_waves(mode, target_flows, thread_count):
      if len(wave) > thread_count:
        waves.extend(_chunk(wave, thread_count))
      else:
        waves.append(wave)
    log_and_print("Mode=%s parallel=%s thread_count=%s batches=%s" % (
      mode, parallel, thread_count, len(waves)), log)
    log_and_print(PARALLEL_OPTIONS[mode]["detail"], log)

    def inject_one(flow):
      try:
        host = host_for(flow["orig_src"])
      except Exception as exc:
        return flow, "?", False, str(exc)
      success, err = inject_flow(flow, host)
      return flow, host, success, err

    def inject_group(group):
      """One CVM login and one root shell for every command in this thread."""
      slot = _borrow_cvm_slot()
      _HELD_SLOT.slot = slot
      try:
        return [inject_one(flow) for flow in group]
      finally:
        _HELD_SLOT.slot = None
        _return_cvm_slot(slot)

    if not args.inspect_only:
      log_and_print("[1/2] Injecting mock flows into conntrack...", log)
      batch_total = len(waves)
      for batch_index, wave in enumerate(waves, 1):
        vms = []
        for flow in wave:
          for ip in (flow["orig_src"], flow["orig_dst"]):
            if ip not in vms:
              vms.append(ip)
        session_count = min(max(1, CVM_SSH_SLOTS), len(wave))
        groups = [wave[index::session_count] for index in range(session_count)]
        log_and_print("Batch %s/%s  mode=%s  thread_count=%s  starting %s flow%s  vms=%s  root_shells=%s" % (
          batch_index, batch_total, mode, thread_count, len(wave),
          "" if len(wave) == 1 else "s", len(vms), session_count), log)
        if session_count <= 1:
          results = inject_group(groups[0])
        else:
          with ThreadPoolExecutor(max_workers=session_count) as pool:
            nested = list(pool.map(inject_group, groups))
          results = [item for group in nested for item in group]
        for flow, host, success, err in results:
          # Store host info for potential flush later
          flow["_host"] = host
          flow["_injected"] = success
          
          if success and err.startswith("updated existing"):
            status = "SUCCESS ({})".format(err)
          elif success:
            status = "SUCCESS"
          else:
            status = "FAILED ({})".format(err)
          log_and_print(
            " -> [{}] ssh root@{} conntrack {} {}:{} -> {}:{}".format(
              status, host, flow["proto"].upper(),
              flow["orig_src"], flow["sport"],
              flow["orig_dst"], flow["dport"],
            ),
            log,
          )
        log_and_print("Batch %s/%s done" % (batch_index, batch_total), log)
    else:
      log_and_print("[1/2] Skipping inject (--inspect_only)", log)

    log_and_print("\n[2/2] Inspecting only injected flows from active table...", log)
    hosts = []
    for flow in target_flows:
      try:
        host = host_for(flow["orig_src"])
      except Exception as exc:
        log_and_print("ERROR choosing hypervisor: {}".format(exc), log)
        continue
      if host not in hosts:
        hosts.append(host)
    for host in hosts:
      log_and_print("ssh root@{} conntrack -L".format(host), log)
      inspect_target_flows(target_flows, host, log)
    
    # Keep-alive mode: periodically refresh flows
    if args.keep_alive and not args.inspect_only:
      log_and_print("\n[3/3] KEEP-ALIVE MODE: Refreshing flows every {}s".format(args.keepalive_interval), log)
      if args.duration_limit:
        log_and_print("  Duration limit: {}s ({}m)".format(args.duration_limit, args.duration_limit // 60), log)
      log_and_print("  Send SIGUSR1 or SIGTERM to trigger graceful teardown", log)
      
      start_time = time.time()
      refresh_count = 0
      
      try:
        while True:
          # Check for teardown signal
          if _TEARDOWN_REQUESTED.is_set():
            log_and_print("\n[TEARDOWN] Signal received, exiting keep-alive loop", log)
            break
          
          # Check duration limit
          if args.duration_limit:
            elapsed = time.time() - start_time
            if elapsed >= args.duration_limit:
              log_and_print("\n[DURATION LIMIT] Reached {}s, auto-tearing down".format(args.duration_limit), log)
              break
          
          # Sleep with interrupt check
          for _ in range(args.keepalive_interval):
            if _TEARDOWN_REQUESTED.is_set():
              break
            time.sleep(1)
          
          if _TEARDOWN_REQUESTED.is_set():
            break
          
          # Refresh flows by updating them
          refresh_count += 1
          log_and_print("\n[REFRESH #{}] Updating {} flows to prevent timeout...".format(
            refresh_count, len(target_flows)), log)
          
          refresh_success = 0
          refresh_failed = 0
          for flow in target_flows:
            if not flow.get("_injected"):
              continue
            host = flow.get("_host")
            if not host:
              continue
            
            try:
              res = run_on_hypervisor(host, update_argv(flow))
              if res.returncode == 0:
                refresh_success += 1
              else:
                refresh_failed += 1
            except Exception:
              refresh_failed += 1
          
          log_and_print("  Refreshed {}/{} flows ({} failed)".format(
            refresh_success, refresh_success + refresh_failed, refresh_failed), log)
          
          elapsed = time.time() - start_time
          log_and_print("  Elapsed: {:.0f}s ({:.1f}m)".format(elapsed, elapsed / 60), log)
          
      except KeyboardInterrupt:
        log_and_print("\n[INTERRUPT] Ctrl+C received, initiating teardown", log)
      except Exception as exc:
        log_and_print("\n[ERROR] Keep-alive exception: {}".format(exc), log)
      
      log_and_print("Keep-alive completed: {} refresh cycles over {:.0f}s".format(
        refresh_count, time.time() - start_time), log)
    
    # Flush flows if requested
    if args.flush_mock or (args.keep_alive and _TEARDOWN_REQUESTED.is_set()):
      injected_flows = [f for f in target_flows if f.get("_injected")]
      if injected_flows:
        log_and_print("\n[CLEANUP] Flushing {} injected mock flows...".format(len(injected_flows)), log)
        flushed, failed = flush_injected_flows(injected_flows, log)
        log_and_print("[CLEANUP] Flush complete: {}/{} removed".format(flushed, len(injected_flows)), log)
    
    log_and_print("\n[COMPLETE] Results logged to: {}\n".format(os.path.abspath(LOG_FILE)), log)


if __name__ == "__main__":
  main()
