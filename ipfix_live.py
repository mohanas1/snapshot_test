"""
Open real TCP and UDP sessions between VMs.

Each direction is repeated for every destination port. Python sockets send a
random size on every write unless payload mode is fixed. iperf3 is used only
when both VMs have it.

Example:
  python3 vm_ipfix_traffic.py --vm-ips 10.37.53.39,10.37.49.190 --duration 30
  python3 vm_ipfix_traffic.py --vm-ips 10.37.53.39,10.37.49.190 --ports 80,443,5201 --proto both
"""

import argparse
import ipaddress
import os
import random
import re
import shlex
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dateutil import tz

try:
    import paramiko
except ImportError:
    sys.exit("paramiko is required: pip install paramiko")


# Lazy-load default IPs to avoid FileNotFoundError at import time
DEFAULT_IPS = []
def _load_default_ips():
    global DEFAULT_IPS
    if not DEFAULT_IPS:
        ips_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ips_on_host.txt")
        if os.path.exists(ips_file):
            DEFAULT_IPS = open(ips_file).read().split()
    return DEFAULT_IPS

# "random" picks a new length on every write. "fixed" repeats PAYLOAD_FIXED.
PAYLOAD_MODE = "random"
PAYLOAD_FIXED = 1400
PAYLOAD_RANDOM_MIN = 1
PAYLOAD_RANDOM_MAX = 1400

# Active schedule. Set this to one name from the blocks below.
#   sequential, same_vm, unique_vms, fan_out, fan_in, spread,
#   first_to_all, all_to_first, randomized_mesh, client_server_pair, port_scan_burst
PARALLEL_MODE = "unique_vms"
# Stop further batches. SIGUSR1 and SIGTERM set this during keep-alive.
_TEARDOWN_REQUESTED = threading.Event()

# sequential — one flow finishes before the next starts
SEQUENTIAL_PARALLEL = False
SEQUENTIAL_THREAD_COUNT = 1

# same_vm — one source and one destination, several ports at once
SAME_VM_PARALLEL = True
SAME_VM_THREAD_COUNT = 5

# unique_vms — each running flow uses two VMs that no other flow is using
UNIQUE_VMS_PARALLEL = True
UNIQUE_VMS_THREAD_COUNT = 20

# fan_out — one source sends to several destinations at once
FAN_OUT_PARALLEL = True
FAN_OUT_THREAD_COUNT = 5

# fan_in — several sources send to one destination at once, each on its own port
FAN_IN_PARALLEL = True
FAN_IN_THREAD_COUNT = 10

# spread — any flows at once; a VM may be in more than one flow
SPREAD_PARALLEL = True
SPREAD_THREAD_COUNT = 5

# first_to_all — first IP sends every port to every other VM
FIRST_TO_ALL_PARALLEL = True
FIRST_TO_ALL_THREAD_COUNT = 10

# all_to_first — every other VM sends every port to the first IP
ALL_TO_FIRST_PARALLEL = True
ALL_TO_FIRST_THREAD_COUNT = 10

# randomized_mesh — the built flows run in a random order
RANDOMIZED_MESH_PARALLEL = True
RANDOMIZED_MESH_THREAD_COUNT = 10

# client_server_pair — first half of the VM list sends to the second half
CLIENT_SERVER_PAIR_PARALLEL = True
CLIENT_SERVER_PAIR_THREAD_COUNT = 10

# port_scan_burst — one source and destination, ports spread across the batch
PORT_SCAN_BURST_PARALLEL = True
PORT_SCAN_BURST_THREAD_COUNT = 10

#nn mode - all source IPs to all destination IPs, all source ports to all destination ports
NN_PARALLEL = True
NN_THREAD_COUNT = 50

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
        "detail": "built flows run in a random order",
    },
    "client_server_pair": {
        "parallel": CLIENT_SERVER_PAIR_PARALLEL,
        "thread_count": CLIENT_SERVER_PAIR_THREAD_COUNT,
        "detail": "first half of the VM list sends to the second half",
    },
        "port_scan_burst": {
            "parallel": PORT_SCAN_BURST_PARALLEL,
            "thread_count": PORT_SCAN_BURST_THREAD_COUNT,
            "detail": "one source and destination, ports spread across the batch",
        },
        "nn": {
            "parallel": NN_PARALLEL,
            "thread_count": NN_THREAD_COUNT,
            "detail": "all source IPs to all destination IPs, all source ports to all destination ports",
        },
}
PRINT_LOCK = threading.Lock()


def log_line(message):
    """Print one status line without interleaving it with another thread."""
    with PRINT_LOCK:
        now = datetime.now(tz=tz.tzlocal())
        print("%s %s" % (now.isoformat(), message), flush=True)
CLIENT_LOCKS = {}
PORT_LOCKS = {}
PORT_LOCKS_GUARD = threading.Lock()


def parse_vm_ips(raw_values, ip_file):
    """Return a de-duplicated list of VM IPv4 addresses."""
    tokens = []
    for value in raw_values or []:
        tokens.extend(value.replace(",", " ").split())
    if not tokens and ip_file:
        with open(ip_file) as handle:
            tokens = handle.read().replace(",", " ").split()
    addresses = []
    seen = set()
    for token in tokens:
        ip = str(ipaddress.ip_address(token))
        if ip not in seen:
            seen.add(ip)
            addresses.append(ip)
    if len(addresses) < 2:
        sys.exit("Need at least two VM IPs via --vm-ips or --ip-file")
    return addresses


def connect(ip, user, password, key_file, ssh_timeout):
    """Open an SSH session. Password and key are optional together."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs = {"hostname": ip, "username": user, "timeout": ssh_timeout, "banner_timeout": ssh_timeout}
    if key_file:
        kwargs["key_filename"] = key_file
    if password:
        kwargs["password"] = password
    try:
        client.connect(**kwargs)
    except Exception:
        client.close()
        raise
    return client


def run(client, command, timeout):
    """Run a remote command and return exit code, stdout, stderr.

    One SSH session is not safe to use from two threads at once, so commands
    on the same VM take turns. The lock covers only the command, so several
    flows can send traffic on that VM at the same time.
    """
    with CLIENT_LOCKS[client]:
        _, stdout, stderr = client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
    return exit_code, out, err


def dest_port_lock(ip, proto, port):
    """One listener per destination, protocol, and port."""
    key = (ip, proto, port)
    with PORT_LOCKS_GUARD:
        if key not in PORT_LOCKS:
            PORT_LOCKS[key] = threading.Lock()
        return PORT_LOCKS[key]


def remote_python(client):
    """Pick a Python binary on the VM."""
    for binary in ("python3", "python"):
        code, out, _ = run(client, "command -v %s" % binary, timeout=15)
        if code == 0 and out.strip():
            return binary
    return None


def has_iperf3(client):
    code, out, _ = run(client, "command -v iperf3", timeout=15)
    return code == 0 and bool(out.strip())


PYTHON_SERVER = r"""
import socket, sys, time
proto = sys.argv[1]
port = int(sys.argv[2])
timeout = int(sys.argv[3])
if proto == "udp":
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass  # SO_REUSEPORT not available on all platforms
    sock.bind(("0.0.0.0", port))
    sock.settimeout(2)
    total = 0
    packets = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data, _addr = sock.recvfrom(65535)
        except socket.timeout:
            if packets:
                break
            continue
        total += len(data)
        packets += 1
    sock.close()
    sys.stdout.write("proto udp bytes_received %s packets %s\n" % (total, packets))
else:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass  # SO_REUSEPORT not available on all platforms
    sock.bind(("0.0.0.0", port))
    sock.listen(1)
    sock.settimeout(timeout)
    try:
        conn, _addr = sock.accept()
    except socket.timeout:
        sys.stderr.write("timeout waiting for connection\n")
        sys.exit(1)
    total = 0
    packets = 0
    conn.settimeout(timeout)
    while True:
        try:
            data = conn.recv(65536)
        except socket.timeout:
            break
        if not data:
            break
        total += len(data)
        packets += 1
    conn.close()
    sock.close()
    sys.stdout.write("proto tcp bytes_received %s packets %s\n" % (total, packets))
"""

PYTHON_CLIENT = r"""
import random, socket, sys, time
proto = sys.argv[1]
dest = sys.argv[2]
port = int(sys.argv[3])
duration = int(sys.argv[4])
payload_mode = sys.argv[5]
payload_min = int(sys.argv[6])
payload_max = int(sys.argv[7])
buffer = b"x" * payload_max
if payload_mode == "fixed":
    payload_label = str(payload_min)
else:
    payload_label = "random %s-%s" % (payload_min, payload_max)

def next_payload():
    if payload_mode == "fixed":
        size = payload_min
    else:
        size = random.randint(payload_min, payload_max)
    return buffer[:size]

def report_source_port(sock):
    src_port = sock.getsockname()[1]
    sys.stdout.write("src_port %s\n" % src_port)
    sys.stdout.flush()
    return src_port

sport = int(sys.argv[8]) if len(sys.argv) > 8 else 0
if proto == "udp":
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass  # SO_REUSEPORT not available on all platforms
    sock.bind(("", sport))
    src_port = report_source_port(sock)
    end = time.time() + duration
    sent = 0
    packets = 0
    while time.time() < end:
        payload = next_payload()
        sock.sendto(payload, (dest, port))
        sent += len(payload)
        packets += 1
        time.sleep(0.01)
    sock.close()
    sys.stdout.write("proto udp src_port %s dst_port %s payload %s bytes_sent %s packets %s\n" % (
        src_port, port, payload_label, sent, packets))
else:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass  # SO_REUSEPORT not available on all platforms
    sock.bind(("", sport))
    sock.settimeout(15)
    sock.connect((dest, port))
    sock.settimeout(None)
    src_port = report_source_port(sock)
    end = time.time() + duration
    sent = 0
    packets = 0
    while time.time() < end:
        payload = next_payload()
        sent += sock.send(payload)
        packets += 1
    sock.close()
    sys.stdout.write("proto tcp src_port %s dst_port %s payload %s bytes_sent %s packets %s\n" % (
        src_port, port, payload_label, sent, packets))
"""


def start_background(client, command, log_path):
    """Start a remote process that survives the SSH channel. Return its pid."""
    wrapped = "nohup %s > %s 2>&1 & echo $!" % (command, shlex.quote(log_path))
    code, out, err = run(client, wrapped, timeout=20)
    if code != 0 or not out.strip().split():
        raise RuntimeError("failed to start background command: %s" % (err or out))
    return int(out.strip().split()[-1])


def wait_pid(client, pid, timeout):
    """Block until the remote pid exits or the timeout elapses."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, _, _ = run(client, "kill -0 %s" % pid, timeout=15)
        if code != 0:
            return True
        time.sleep(1)
    run(client, "kill %s" % pid, timeout=15)
    return False


def stop_pid(client, pid):
    if pid:
        run(client, "kill %s 2>/dev/null || true" % pid, timeout=15)


def read_remote_file(client, path):
    code, out, _ = run(client, "cat %s" % shlex.quote(path), timeout=20)
    return out if code == 0 else ""


def start_server(client, python_bin, use_iperf, proto, port, ready_timeout):
    """Listen on the destination VM. Return pid and log path."""
    log_path = "/tmp/vm_flow_srv_%s_%s.log" % (proto, port)
    run(client, "rm -f %s" % shlex.quote(log_path), timeout=15)
    
    # Kill any existing process on this port to prevent "Address already in use"
    proto_flag = "tcp" if proto == "tcp" else "udp"
    run(client, "fuser -k %s/%s 2>/dev/null || true" % (port, proto_flag), timeout=10)
    time.sleep(0.3)  # Give kernel time to release the port
    
    if use_iperf:
        command = "iperf3 -s -1 -p %s" % port
    else:
        command = "%s -c %s %s %s %s" % (
            python_bin, shlex.quote(PYTHON_SERVER), proto, port, ready_timeout)
    pid = start_background(client, command, log_path)
    time.sleep(1)
    code, _, _ = run(client, "kill -0 %s" % pid, timeout=15)
    if code != 0:
        detail = read_remote_file(client, log_path).strip()
        raise RuntimeError("server exited immediately: %s" % (detail or "no log"))
    return pid, log_path


def start_client(client, python_bin, use_iperf, proto, dest_ip, port, duration, payload_mode, payload_min, payload_max, sport=0):
    """Send traffic from the source VM. Return pid and log path."""
    log_path = "/tmp/vm_flow_cli_%s_%s_%s_%s.log" % (proto, dest_ip.replace(".", "_"), port, sport or 0)
    run(client, "rm -f %s" % shlex.quote(log_path), timeout=15)
    if use_iperf:
        command = "iperf3 -c %s -t %s -p %s" % (dest_ip, duration, port)
        if sport:
            command += " --cport %s" % sport
        if proto == "udp":
            command += " -u -b 1M"
    else:
        command = "%s -u -c %s %s %s %s %s %s %s %s %s" % (
            python_bin, shlex.quote(PYTHON_CLIENT), proto, dest_ip, port, duration,
            payload_mode, payload_min, payload_max, sport or 0)
    pid = start_background(client, command, log_path)
    return pid, log_path


def read_source_port(client, log_path, timeout=25):
    """Return the client source port once the remote log records it."""
    pattern = re.compile(r"(?:^|\n)src_port (\d+)\b|local \S+ port (\d+)")
    deadline = time.time() + timeout
    attempts = 0
    while time.time() < deadline:
        # Force filesystem sync on every 5th attempt
        if attempts > 0 and attempts % 5 == 0:
            run(client, "sync", timeout=5)
        match = pattern.search(read_remote_file(client, log_path))
        if match:
            return int(match.group(1) or match.group(2))
        time.sleep(0.3)
        attempts += 1
    return None


def payload_label(payload_mode, payload_min, payload_max):
    """Text for logs. Fixed is one size. Random is a new size on every write."""
    if payload_mode == "fixed":
        return "%s bytes every write" % payload_min
    return "random %s-%s bytes every write" % (payload_min, payload_max)


def _group_in_order(jobs, keyfn):
    """Bucket jobs in the order their keys first appear."""
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
    """Take one job from each key in turn."""
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
    """Pack jobs into waves. join_keys returns keys the job occupies, or None to skip it."""
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


def build_waves(mode, jobs, width):
    """Group flows so concurrent ones match the selected schedule."""
    if mode == "sequential" or width <= 1:
        return [[job] for job in jobs]
    if mode == "same_vm":
        waves = []
        for group in _group_in_order(jobs, lambda job: (job[0]["ip"], job[1]["ip"])):
            waves.extend(_chunk(group, width))
        return waves
    if mode == "unique_vms":
        def unique_vm_keys(job, used):
            src_ip = job[0]["ip"]
            dst_ip = job[1]["ip"]
            if src_ip in used or dst_ip in used:
                return None
            return (src_ip, dst_ip)
        return _select_waves(jobs, width, unique_vm_keys)
    if mode == "fan_out":
        waves = []
        for group in _group_in_order(jobs, lambda job: job[0]["ip"]):
            mixed = _interleave(group, lambda job: job[1]["ip"])

            def fan_out_keys(job, used):
                dst_ip = job[1]["ip"]
                if dst_ip in used:
                    return None
                return (dst_ip,)
            waves.extend(_select_waves(mixed, width, fan_out_keys))
        return waves
    if mode == "fan_in":
        waves = []
        for group in _group_in_order(jobs, lambda job: job[1]["ip"]):
            mixed = _interleave(group, lambda job: job[0]["ip"])

            def fan_in_keys(job, used):
                src_ip = job[0]["ip"]
                port_key = ("port", job[2], job[3])
                if src_ip in used or port_key in used:
                    return None
                return (src_ip, port_key)
            waves.extend(_select_waves(mixed, width, fan_in_keys))
        return waves
    if mode == "spread":
        return [_interleave(jobs, lambda job: (job[0]["ip"], job[1]["ip"]))]
    if mode == "first_to_all":
        mixed = _interleave(jobs, lambda job: job[1]["ip"])

        def first_to_all_keys(job, used):
            dst_ip = job[1]["ip"]
            if dst_ip in used:
                return None
            return (dst_ip,)
        return _select_waves(mixed, width, first_to_all_keys)
    if mode == "all_to_first":
        mixed = _interleave(jobs, lambda job: job[0]["ip"])

        def all_to_first_keys(job, used):
            src_ip = job[0]["ip"]
            if src_ip in used:
                return None
            return (src_ip,)
        return _select_waves(mixed, width, all_to_first_keys)
    if mode == "randomized_mesh":
        shuffled = list(jobs)
        random.shuffle(shuffled)
        return _chunk(shuffled, width)
    if mode == "client_server_pair":
        if not jobs:
            return []
        vm_list = sorted({job[0]["ip"] for job in jobs} | {job[1]["ip"] for job in jobs})
        midpoint = max(1, len(vm_list) // 2)
        clients = set(vm_list[:midpoint])
        servers = set(vm_list[midpoint:])
        filtered = [job for job in jobs if job[0]["ip"] in clients and job[1]["ip"] in servers]
        if not filtered:
            filtered = list(jobs)
        return _chunk(filtered, width)
    if mode == "port_scan_burst":
        waves = []
        for group in _group_in_order(jobs, lambda job: (job[0]["ip"], job[1]["ip"])):
            mixed = _interleave(group, lambda job: job[3])
            waves.extend(_chunk(mixed, width))
        return waves
    if mode == "nn":
        # For nn mode, jobs are already prepared with all combinations.
        # We just need to chunk them into waves.
        return _chunk(jobs, width)
    sys.exit("Unknown parallel mode %r. Choose one of: %s" % (
        mode, ", ".join(PARALLEL_OPTIONS)))


def flow_once(src, dst, python_bins, iperf_ok, proto, port, duration, payload_mode, payload_min, payload_max, traffic, sport=0):
    """One direction: server on dst, client on src."""
    with dest_port_lock(dst["ip"], proto, port):
        return _flow_once(
            src, dst, python_bins, iperf_ok, proto, port, duration,
            payload_mode, payload_min, payload_max, traffic, sport)


def _flow_once(src, dst, python_bins, iperf_ok, proto, port, duration, payload_mode, payload_min, payload_max, traffic, sport=0):
    """Run one flow. SSH on a VM is serialized; the traffic itself overlaps."""
    use_iperf = traffic == "iperf3" or (traffic == "auto" and iperf_ok[src["ip"]] and iperf_ok[dst["ip"]])
    if traffic == "iperf3" and not use_iperf:
        raise RuntimeError("iperf3 is missing on %s or %s" % (src["ip"], dst["ip"]))
    tool = "iperf3" if use_iperf else "python"
    if use_iperf and proto == "udp":
        size = "1 Mbit/s iperf3"
    elif use_iperf:
        size = "iperf3 default block"
    else:
        size = payload_label(payload_mode, payload_min, payload_max)
    ready_timeout = duration + 20
    server_pid = None
    client_pid = None
    logged = False
    result = {
        "src": src["ip"],
        "dst": dst["ip"],
        "proto": proto.upper(),
        "tool": tool,
        "port": port,
        "src_port": None,
        "payload": size if tool == "python" else ("1 Mbit/s" if proto == "udp" else "iperf3 default"),
        "duration": duration,
        "client_log": "",
        "server_log": "",
        "finished": False,
        "error": "",
    }
    try:
        server_pid, server_log = start_server(
            dst["client"], python_bins[dst["ip"]], use_iperf, proto, port, ready_timeout)
        client_pid, client_log = start_client(
            src["client"], python_bins[src["ip"]], use_iperf, proto, dst["ip"], port, duration,
            payload_mode, payload_min, payload_max, sport)
        result["src_port"] = read_source_port(src["client"], client_log)
        src_port_text = result["src_port"] if result["src_port"] else "?"
        log_line("%s %s:%s -> %s:%s  %s  duration=%ss" % (
            proto.upper(), src["ip"], src_port_text, dst["ip"], port, size, duration))
        logged = True
        result["finished"] = wait_pid(src["client"], client_pid, duration + 25)
        result["client_log"] = read_remote_file(src["client"], client_log).strip()
        result["server_log"] = read_remote_file(dst["client"], server_log).strip()
    except Exception as exc:
        result["error"] = str(exc)
        if not logged:
            log_line("%s %s:? -> %s:%s  %s  duration=%ss  error=%s" % (
                proto.upper(), src["ip"], dst["ip"], port, size, duration, exc))
    finally:
        stop_pid(dst["client"], server_pid)
        stop_pid(src["client"], client_pid)
    return result


def print_summary(results):
    print("")
    print("Flow summary")
    for item in results:
        print("- %s %s:%s -> %s:%s  tool=%s payload=%s duration=%ss finished=%s" % (
            item["proto"], item["src"], item["src_port"] if item["src_port"] else "?",
            item["dst"], item["port"], item["tool"],
            item["payload"], item["duration"], item["finished"]))
        if item["error"]:
            print("  error: %s" % item["error"])
        if item["client_log"]:
            tail = item["client_log"].splitlines()[-2:]
            print("  sent: %s" % " | ".join(tail))
        if item["server_log"]:
            tail = item["server_log"].splitlines()[-2:]
            print("  received: %s" % " | ".join(tail))


def parse_port_list(raw):
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


def ports_for_proto(proto, shared_ports, tcp_ports, udp_ports):
    """Destination ports for one protocol. A specific list wins over --ports."""
    if tcp_ports or udp_ports:
        chosen = list(tcp_ports or []) if proto == "tcp" else list(udp_ports or [])
        if chosen:
            return chosen
    return list(shared_ports)


def build_directions(sessions, pattern, n):
    """VM pairs for one pass. mesh is every unique pair in both directions."""
    if len(sessions) < 2:
        sys.exit("Need at least two VM IPs via --vm-ips or --ip-file")
    if pattern == "mesh":
        directions = []
        for index, src in enumerate(sessions):
            for dst in sessions[index + 1:]:
                directions.append((src, dst))
                directions.append((dst, src))
        return directions
    n = max(1, min(n, len(sessions) - 1))
    if pattern == "1:1":
        return [(sessions[i * 2], sessions[i * 2 + 1]) for i in range(len(sessions) // 2)]
    if pattern == "1:n":
        return [(sessions[0], dst) for dst in sessions[1:1 + n]]
    if pattern == "n:1":
        return [(src, sessions[0]) for src in sessions[1:1 + n]]
    if pattern == "1:all":
        return [(sessions[0], dst) for dst in sessions[1:]]
    if pattern == "all:1":
        return [(src, sessions[0]) for src in sessions[1:]]
    if pattern == "nn":
        directions = []
        for src in sessions:
            for dst in sessions:
                directions.append((src, dst))
        return directions
    sys.exit("Unknown pattern %r" % pattern)


def _request_teardown(signum, _frame):
    log_line("Received signal %s, stopping after the current batch" % signum)
    _TEARDOWN_REQUESTED.set()


def main():
    parser = argparse.ArgumentParser(description="Generate real TCP and UDP flows between VMs")
    parser.add_argument("--vm-ips", action="append", default=[],
                        help="Comma-separated VM IPs. Repeat or combine. Default: --ip-file")
    parser.add_argument("--ip-file", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ips_on_host.txt"),
                        help="Whitespace-separated VM IPs used when --vm-ips is omitted")
    parser.add_argument("--ssh-user", default="root", help="SSH user on every test VM")
    parser.add_argument("--ssh-password", default="nutanix/4u",
                        help="SSH password for the VMs. Defaults to $VM_SSH_PASSWORD")
    parser.add_argument("--ssh-key", default=None, help="Private key for the VMs")
    parser.add_argument("--pattern", choices=("mesh", "1:1", "1:n", "n:1", "1:all", "all:1", "nn"), default="mesh",
                        help="Which VM pairs talk. mesh is every pair in both directions. "
                             "1:all and all:1 use the first IP as the hub.")
    parser.add_argument("--n", type=int, default=10, help="How many peers for 1:n and n:1. Default: 10")
    parser.add_argument("--ports", default="80,443,53,123,5201",
                        help="Destination ports as commas or a start-end range. Default: 80,443,53,123,5201")
    parser.add_argument("--tcp-ports", default="", help="TCP destination ports. Overrides --ports for TCP.")
    parser.add_argument("--udp-ports", default="", help="UDP destination ports. Overrides --ports for UDP.")
    parser.add_argument("--sports", default="",
                        help="Source ports as commas or a start-end range. Empty lets the VM pick a port.")
    parser.add_argument("--proto", choices=("tcp", "udp", "both"), default="both",
                        help="Transport to generate. Default: both")
    parser.add_argument("--payload-mode", choices=("random", "fixed"), default=PAYLOAD_MODE,
                        help="random picks a new size on every write. fixed repeats --payload. "
                             "Default: %s" % PAYLOAD_MODE)
    parser.add_argument("--payload", type=int, default=PAYLOAD_FIXED,
                        help="Bytes per write when --payload-mode is fixed. Default: %s" % PAYLOAD_FIXED)
    parser.add_argument("--payload-min", type=int, default=PAYLOAD_RANDOM_MIN,
                        help="Smallest random write. Default: %s" % PAYLOAD_RANDOM_MIN)
    parser.add_argument("--payload-max", type=int, default=PAYLOAD_RANDOM_MAX,
                        help="Largest random write. Default: %s" % PAYLOAD_RANDOM_MAX)
    parser.add_argument("--duration", type=int, default=1, help="Seconds of traffic per flow")
    parser.add_argument("--timeout", type=int, default=20, help="SSH connection timeout in seconds")
    parser.add_argument("--traffic", choices=("auto", "iperf3", "python"), default="auto",
                        help="auto uses iperf3 when both VMs have it, otherwise Python sockets")
    parser.add_argument("--parallel-mode", default=PARALLEL_MODE, choices=tuple(PARALLEL_OPTIONS),
                        help="How flows share VMs. Default: %s. Edit the matching *_PARALLEL "
                             "and *_THREAD_COUNT variables to change each option." % PARALLEL_MODE)
    parser.add_argument("--parallel", action=argparse.BooleanOptionalAction, default=None,
                        help="Override the selected mode's *_PARALLEL flag")
    parser.add_argument("--thread-count", type=int, default=None,
                        help="Override the selected mode's *_THREAD_COUNT")
    parser.add_argument("--keep-alive", action="store_true",
                        help="Repeat the live flows until --duration-limit or a stop signal")
    parser.add_argument("--keepalive-interval", type=int, default=60,
                        help="Seconds to wait between keep-alive passes. Default: 60")
    parser.add_argument("--duration-limit", type=int, default=None,
                        help="Stop keep-alive after this many seconds")
    args = parser.parse_args()

    if args.parallel_mode not in PARALLEL_OPTIONS:
        sys.exit("Unknown --parallel-mode %r" % args.parallel_mode)
    option = PARALLEL_OPTIONS[args.parallel_mode]
    parallel = option["parallel"] if args.parallel is None else args.parallel
    mode = args.parallel_mode if parallel else "sequential"
    thread_count = option["thread_count"] if args.thread_count is None else args.thread_count
    if not parallel:
        thread_count = 1

    if args.duration < 1 or args.duration > 600:
        sys.exit("--duration must be between 1 and 600")
    if args.payload_mode == "fixed":
        if not 1 <= args.payload <= 60000:
            sys.exit("--payload must be between 1 and 60000")
        payload_min = payload_max = args.payload
    else:
        if not 1 <= args.payload_min <= args.payload_max <= 60000:
            sys.exit("--payload-min and --payload-max must satisfy 1 <= min <= max <= 60000")
        payload_min = args.payload_min
        payload_max = args.payload_max
    size_label = payload_label(args.payload_mode, payload_min, payload_max)
    if thread_count < 1:
        sys.exit("--thread-count must be at least 1")
    if args.parallel_mode == "first_to_all":
        pattern = "1:all"
    elif args.parallel_mode == "all_to_first":
        pattern = "all:1"
    else:
        pattern = args.pattern
    shared_ports = parse_port_list(args.ports)
    tcp_ports = parse_port_list(args.tcp_ports)
    udp_ports = parse_port_list(args.udp_ports)
    sports = parse_port_list(args.sports)
    protos = ["tcp", "udp"] if args.proto == "both" else [args.proto]
    ports_by_proto = {}
    for proto in protos:
        chosen = ports_for_proto(proto, shared_ports, tcp_ports, udp_ports)
        if not chosen:
            sys.exit("No destination ports for %s. Set --ports or --%s-ports." % (proto, proto))
        ports_by_proto[proto] = chosen
    if args.keep_alive and args.keepalive_interval < 0:
        sys.exit("--keepalive-interval must be 0 or greater")
    if args.duration_limit is not None and args.duration_limit < 1:
        sys.exit("--duration-limit must be at least 1")
    if not args.ssh_password and not args.ssh_key:
        sys.exit("Pass --ssh-password or --ssh-key (or set $VM_SSH_PASSWORD)")

    vm_ips = parse_vm_ips(args.vm_ips, args.ip_file if not args.vm_ips else None)
    sessions = [{"ip": ip, "client": None} for ip in vm_ips]
    python_bins = {}
    iperf_ok = {}
    skipped_vm_reasons = {}

    def close_session(session):
        client = session.get("client")
        if client is None:
            return
        try:
            client.close()
        finally:
            CLIENT_LOCKS.pop(client, None)
            session["client"] = None

    def connect_session(session):
        """Open SSH for one VM. An authentication failure removes the VM."""
        if session["ip"] in skipped_vm_reasons:
            return
        log_line("Connecting to VM %s" % session["ip"])
        try:
            client = connect(session["ip"], args.ssh_user, args.ssh_password, args.ssh_key, args.timeout)
        except paramiko.AuthenticationException as exc:
            skipped_vm_reasons[session["ip"]] = f"authentication failed ({exc})"
            log_line("Skipping VM %s: authentication failed (%s)" % (session["ip"], exc))
            return
        except (TimeoutError, Exception) as exc:
            skipped_vm_reasons[session["ip"]] = f"connection failed ({exc})"
            log_line("Skipping VM %s: connection failed (%s)" % (session["ip"], exc))
            return
        session["client"] = client
        CLIENT_LOCKS[client] = threading.Lock()
        try:
            if session["ip"] not in python_bins:
                python_bin = remote_python(client)
                iperf = has_iperf3(client)
                if python_bin is None and not iperf:
                    raise RuntimeError("VM %s has neither python nor iperf3" % session["ip"])
                python_bins[session["ip"]] = python_bin
                iperf_ok[session["ip"]] = iperf
                log_line("VM %s python=%s iperf3=%s" % (session["ip"], python_bin, iperf))
                
                # Clean up any orphaned server processes from previous aborted runs
                log_line("Cleaning up orphaned processes on VM %s" % session["ip"])
                run(client, "pkill -9 -f 'python.*PYTHON_SERVER' 2>/dev/null || true", timeout=10)
                run(client, "pkill -9 -f 'iperf3 -s' 2>/dev/null || true", timeout=10)
                # Also clean up test client processes that might be stuck
                run(client, "pkill -9 -f 'python.*PYTHON_CLIENT' 2>/dev/null || true", timeout=10)
        except Exception:
            close_session(session)
            raise

    def use_sessions(needed):
        """Keep SSH open for all VMs used in the entire job."""
        needed = [item for item in needed if item["ip"] not in skipped_vm_reasons]
        # Don't close any connections - keep them alive for reuse
        missing = [item for item in needed if item["client"] is None]
        if not missing:
            return
        log_line("Connecting to %s VM%s in this batch" % (len(missing), "" if len(missing) == 1 else "s"))
        with ThreadPoolExecutor(max_workers=len(missing)) as pool:
            futures = [pool.submit(connect_session, item) for item in missing]
            for future in as_completed(futures):
                future.result()

    try:
        directions = build_directions(sessions, pattern, args.n)
        jobs = []
        for src_item, dst_item in directions:
            for proto in protos:
                for port in ports_by_proto[proto]:
                    sport = sports[len(jobs) % len(sports)] if sports else 0
                    jobs.append((src_item, dst_item, proto, port, sport))
        port_text = ",".join(
            "%s:%s" % (proto, ",".join(str(port) for port in ports_by_proto[proto]))
            for proto in protos)
        print("Plan: %s flows, pattern=%s, protocols=%s, dest ports=%s, payload=%s, duration=%ss each" % (
            len(jobs), pattern, ",".join(proto.upper() for proto in protos),
            port_text, size_label, args.duration))
        print("UDP python sends one datagram every 10ms. TCP python writes until the duration ends. Each write is %s." % (
            size_label))
        waves = []
        for wave in build_waves(mode, jobs, thread_count):
            if len(wave) > thread_count:
                waves.extend(_chunk(wave, thread_count))
            else:
                waves.append(wave)
        print("Mode=%s parallel=%s thread_count=%s batches=%s" % (
            mode, parallel, thread_count, len(waves)))
        print(PARALLEL_OPTIONS[mode]["detail"])
        
        # Precompute all VMs used across all batches (for keep-alive mode)
        all_used_ips = set()
        if args.keep_alive:
            for wave in waves:
                for job in wave:
                    all_used_ips.add(job[0]["ip"])
                    all_used_ips.add(job[1]["ip"])
            all_used_ips.discard(None)
            log_line("Will use %s unique VM%s total across all batches - keeping connections alive" % 
                     (len(all_used_ips), "" if len(all_used_ips) == 1 else "s"))
        
        print("SSH opens only for the VMs in the current batch.")

        def run_job(job):
            src_item, dst_item, proto, port, sport = job
            return flow_once(
                src_item, dst_item, python_bins, iperf_ok, proto, port,
                args.duration, args.payload_mode, payload_min, payload_max, args.traffic, sport)

        results = []
        batch_total = len(waves)

        def usable_jobs(wave):
            return [job for job in wave
                    if job[0]["ip"] not in skipped_vm_reasons and job[1]["ip"] not in skipped_vm_reasons]

        def sessions_for(wave):
            needed = []
            seen = set()
            for job in wave:
                for session in (job[0], job[1]):
                    if session["ip"] in seen or session["ip"] in skipped_vm_reasons:
                        continue
                    seen.add(session["ip"])
                    needed.append(session)
            return needed

        def execute_keep_alive_mode():
            """
            Keep-alive mode: Each flow maintained by independent persistent worker.
            Workers run in autonomous loops: send traffic → wait → repeat.
            """
            # Keep-alive mode: background worker threads with ThreadPoolExecutor
            signal.signal(signal.SIGUSR1, _request_teardown)
            signal.signal(signal.SIGTERM, _request_teardown)
            log_line("Keep-alive mode: autonomous background workers with continuous refresh")
            log_line("Flow duration=%ss, Refresh interval=%ss. Send SIGUSR1 or SIGTERM to stop." % 
                     (args.duration, args.keepalive_interval))
            if args.duration_limit:
                log_line("Duration limit: %ss" % args.duration_limit)

            # Connect to all required VMs upfront for keep-alive mode
            all_sessions_needed = [s for s in sessions if s["ip"] in all_used_ips and s["ip"] not in skipped_vm_reasons]
            if all_sessions_needed:
                log_line("Opening SSH connections to all %s VM%s" % 
                         (len(all_sessions_needed), "" if len(all_sessions_needed) == 1 else "s"))
                use_sessions(all_sessions_needed)

            # Flatten all jobs into a single list for persistent workers
            all_jobs = []
            for wave in waves:
                all_jobs.extend(usable_jobs(wave))
            
            log_line("Starting %s flow%s with ThreadPoolExecutor (max_workers=%s)" % (
                len(all_jobs), "" if len(all_jobs) == 1 else "s", thread_count))
            
            started = time.time()
            worker_results = {}
            worker_results_lock = threading.Lock()
            
            def autonomous_worker(job_id, job):
                """
                Autonomous worker running in continuous loop.
                Executes traffic for --duration, sleeps for --keepalive-interval, repeats.
                """
                src_item, dst_item, proto, port, sport = job
                refresh_count = 0
                worker_start = time.time()
                last_result = None
                
                # Continuous autonomous loop
                while not _TEARDOWN_REQUESTED.is_set():
                    # Check duration limit
                    if args.duration_limit and (time.time() - started) >= args.duration_limit:
                        break
                    
                    # Execute traffic flow for configured --duration (e.g., 30s)
                    try:
                        refresh_count += 1
                        last_result = flow_once(
                            src_item, dst_item, python_bins, iperf_ok, proto, port,
                            args.duration, args.payload_mode, payload_min, payload_max, args.traffic, sport)
                    except Exception as e:
                        last_result = {
                            "error": str(e),
                            "finished": False,
                            "src_ip": src_item.get("ip"),
                            "dst_ip": dst_item.get("ip"),
                            "proto": proto
                        }
                    
                    # Update results (thread-safe)
                    with worker_results_lock:
                        worker_results[job_id] = {
                            "result": last_result,
                            "refresh_count": refresh_count,
                            "elapsed": time.time() - worker_start
                        }
                    
                    # Check if we should continue
                    if not args.keep_alive or _TEARDOWN_REQUESTED.is_set():
                        break
                    
                    # Sleep/wait for keepalive-interval (e.g., 60s), checking teardown flag periodically
                    for _ in range(args.keepalive_interval):
                        if _TEARDOWN_REQUESTED.is_set():
                            break
                        time.sleep(1)  # Sleep in 1s increments to check teardown flag
                
                return job_id
            
            # Launch all workers as background threads using ThreadPoolExecutor
            # Workers run autonomously without blocking or waiting for others
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                log_line("Submitting %s autonomous worker%s to thread pool" % (
                    len(all_jobs), "" if len(all_jobs) == 1 else "s"))
                
                # Submit all jobs to executor - they run autonomously
                futures = [executor.submit(autonomous_worker, idx, job) 
                          for idx, job in enumerate(all_jobs)]
                
                log_line("All workers submitted and running autonomously")
                
                # Wait for all workers to complete (or teardown signal)
                for future in as_completed(futures):
                    try:
                        job_id = future.result()
                    except Exception as e:
                        log_line("Worker exception: %s" % str(e))
            
            # Collect results
            results = []
            log_line("All autonomous workers completed")
            for job_id in sorted(worker_results.keys()):
                info = worker_results[job_id]
                if info["result"]:
                    results.append(info["result"])
                if info["refresh_count"] > 1:
                    log_line("Flow %s: %s refresh cycle%s over %.1fs" % (
                        job_id, info["refresh_count"], "" if info["refresh_count"] == 1 else "s",
                        info["elapsed"]))
            
            return results

        def execute_one_shot_mode():
            """
            One-shot mode: Original sequential batch execution.
            Processes flows in waves with ThreadPoolExecutor.
            """
            results = []
            started = time.time()
            for batch_index, wave in enumerate(waves, 1):
                if _TEARDOWN_REQUESTED.is_set():
                    log_line("Stop requested, leaving remaining batches")
                    break
                pending = usable_jobs(wave)
                if not pending:
                    log_line("Batch %s/%s skipped, every VM in it was removed" % (batch_index, batch_total))
                    continue
                use_sessions(sessions_for(pending))
                pending = usable_jobs(pending)
                if not pending:
                    log_line("Batch %s/%s skipped after authentication failure" % (batch_index, batch_total))
                    use_sessions([])
                    continue
                needed = sessions_for(pending)
                use_sessions(needed)
                slots = min(thread_count, len(pending))
                log_line("Batch %s/%s  mode=%s  thread_count=%s  starting %s flow%s  vms=%s" % (
                    batch_index, batch_total, mode, thread_count, len(pending),
                    "" if len(pending) == 1 else "s", len(needed)))
                if slots <= 1:
                    for job in pending:
                        results.append(run_job(job))
                else:
                    with ThreadPoolExecutor(max_workers=slots) as pool:
                        futures = [pool.submit(run_job, job) for job in pending]
                        for future in as_completed(futures):
                            results.append(future.result())
                log_line("Batch %s/%s done" % (batch_index, batch_total))
            
            return results

        # ===================================================================
        # EXECUTION ROUTING: Keep-Alive vs One-Shot Mode
        # ===================================================================
        
        if args.keep_alive:
            # Route to dedicated keep-alive execution path
            results = execute_keep_alive_mode()
        else:
            # Route to original one-shot sequential batch execution
            results = execute_one_shot_mode()
        
        # ===================================================================
        # POST-EXECUTION: Common cleanup and reporting (both modes)
        # ===================================================================
        
        if skipped_vm_reasons:
            reasons_count = {}
            for ip, reason in skipped_vm_reasons.items():
                reasons_count[reason] = reasons_count.get(reason, 0) + 1
            
            log_line("Removed %s VM%s after connection failures:" % (
                len(skipped_vm_reasons), "" if len(skipped_vm_reasons) == 1 else "s"))
            for reason, count in reasons_count.items():
                log_line("  - %s VM%s due to %s" % (count, "" if count == 1 else "s", reason))
        print_summary(results)
        failures = [item for item in results if item["error"] or not item["finished"]]
        return 1 if failures else 0
    finally:
        for session in sessions:
            close_session(session)


if __name__ == "__main__":
    sys.exit(main())
