# Service Manager Update - Port Conflict Auto-Resolution

## Overview
Updated `service-manager.sh` to automatically detect and kill any processes blocking port 8765 before starting or restarting the Flask service.

## Problem
The systemd service would fail to start with exit code 1 when port 8765 was already in use:
```
Active: activating (auto-restart) (Result: exit-code)
Process: ExecStart=/usr/local/bin/python3.12 app.py (code=exited, status=1/FAILURE)
```

This commonly occurred when:
- A previous Flask process didn't shut down cleanly
- Manual `nohup python3.12 app.py` commands were still running
- The service was restarted too quickly without proper cleanup

## Solution

### Added `kill_port_process()` Function
The script now includes an automatic port cleanup function that:
1. Checks if port 8765 is in use (`lsof -ti:8765`)
2. Kills any process(es) blocking the port (`kill -9`)
3. Verifies the port is now free
4. Provides clear visual feedback with emojis

### Updated Commands
- **`start`** - Now kills port conflicts before starting
- **`restart`** - Now kills port conflicts before restarting
- **`kill-port`** - New command to manually kill port conflicts

## Usage

### Basic Usage (Recommended)
```bash
# Restart the service (auto-kills port conflicts)
bash service-manager.sh restart

# Start the service (auto-kills port conflicts)
bash service-manager.sh start

# Stop the service
bash service-manager.sh stop

# Check service status
bash service-manager.sh status
```

### New Command: Manual Port Cleanup
```bash
# Kill any process using port 8765 without starting service
bash service-manager.sh kill-port
```

### Other Commands
```bash
# View live logs
bash service-manager.sh logs

# View last 50 log lines
bash service-manager.sh logs-tail

# Enable service to start on boot
bash service-manager.sh enable

# Disable service from starting on boot
bash service-manager.sh disable
```

## Example Output

### Successful Restart with Port Conflict
```
Restarting bulk-snapshots-ui...
⚠️  Port 8765 is already in use by process(es): 1213866
🔪 Killing process(es) on port 8765...
✅ Port 8765 is now free
● bulk-snapshots-ui.service - Bulk VM Snapshots UI - Flask Web Application
   Loaded: loaded (/etc/systemd/system/bulk-snapshots-ui.service; enabled)
   Active: active (running) since Fri 2026-06-05 04:37:05 UTC; 2s ago
```

### Successful Restart without Port Conflict
```
Restarting bulk-snapshots-ui...
✅ Port 8765 is available
● bulk-snapshots-ui.service - Bulk VM Snapshots UI - Flask Web Application
   Loaded: loaded (/etc/systemd/system/bulk-snapshots-ui.service; enabled)
   Active: active (running)
```

## Deprecated Script
`RESTART_FLASK.sh` has been deprecated and now redirects to `service-manager.sh restart`. It's kept for backward compatibility only.

## Benefits
1. **Zero-friction restarts** - No manual process killing required
2. **Idempotent operations** - Can safely run `restart` multiple times
3. **Clear feedback** - Visual indicators show what's happening
4. **Safer operations** - Verifies port is free before proceeding
5. **Systemd integration** - Works properly with the systemd service

## Technical Details

### Port Detection
```bash
sudo lsof -ti:$PORT 2>/dev/null
```
- `lsof -ti:8765` lists process IDs using port 8765
- Redirects stderr to /dev/null to suppress errors when port is free

### Process Termination
```bash
echo "$pids" | xargs -r sudo kill -9
```
- Uses `kill -9` (SIGKILL) for immediate termination
- `-r` flag prevents xargs from running if input is empty

### Verification
After killing, the script verifies the port is free and reports failure if processes persist.

## Related Files
- `/home/mohan.as1/mohan_helpers/bulk_snapshots_ui/service-manager.sh` (updated)
- `/home/mohan.as1/mohan_helpers/bulk_snapshots_ui/RESTART_FLASK.sh` (deprecated)
- `/etc/systemd/system/bulk-snapshots-ui.service` (systemd unit file)

## Date Updated
2026-06-05
