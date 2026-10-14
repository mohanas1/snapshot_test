#!/bin/bash
# Quick management script for bulk-snapshots-ui service

SERVICE_NAME="bulk-snapshots-ui"
PORT=8766

# Function to kill any process running on the service port
kill_port_process() {
    local pids=$(sudo lsof -ti:$PORT 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "⚠️  Port $PORT is already in use by process(es): $pids"
        echo "🔪 Killing process(es) on port $PORT..."
        echo "$pids" | xargs -r sudo kill -9
        sleep 1

        # Verify the port is now free
        local check_pids=$(sudo lsof -ti:$PORT 2>/dev/null)
        if [ -n "$check_pids" ]; then
            echo "❌ Failed to kill process(es) on port $PORT: $check_pids"
            return 1
        else
            echo "✅ Port $PORT is now free"
        fi
    else
        echo "✅ Port $PORT is available"
    fi
    return 0
}

# Check for in-progress jobs via the API.
# Returns 0 (safe to proceed) or 1 (user said no / API unreachable with jobs running).
check_running_jobs() {
    local api_url="http://localhost:${PORT}/api/running-jobs"

    # Try to reach the API (2s timeout, 1 retry)
    local response
    response=$(curl -sf --max-time 3 "$api_url" 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo "ℹ️  Service is not reachable at $api_url — assuming no running jobs."
        return 0
    fi

    # Parse count with python (always available on the host)
    local count
    count=$(echo "$response" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    jobs = d.get('jobs', [])
    print(len(jobs))
    for j in jobs:
        feat  = j.get('feature', '?')
        rid   = j.get('run_id', '?')
        st    = j.get('status', '?')
        pc    = j.get('pc_host', '')
        ts    = j.get('started_at', '')
        parts = [f'  • [{feat}] {rid}  status={st}']
        if pc:  parts.append(f'pc={pc}')
        if ts:  parts.append(f'started={ts}')
        print(' '.join(parts), file=sys.stderr)
except Exception as e:
    print(0)
" 2>/tmp/_smgr_jobs.txt)

    if [ -z "$count" ] || [ "$count" -eq 0 ]; then
        echo "✅ No running jobs."
        return 0
    fi

    echo ""
    echo "⚠️  There are $count in-progress job(s):"
    cat /tmp/_smgr_jobs.txt
    echo ""
    read -r -p "❓ Continue anyway? [y/N] " answer
    case "$answer" in
        [yY][eE][sS]|[yY]) return 0 ;;
        *) echo "🚫 Aborted."; return 1 ;;
    esac
}

case "$1" in
    start)
        echo "Starting $SERVICE_NAME..."
        # kill_port_process
        sudo systemctl start "$SERVICE_NAME"
        sleep 1
        sudo systemctl status "$SERVICE_NAME" --no-pager
        ;;
    stop)
        echo "Checking for in-progress jobs before stopping..."
        check_running_jobs || exit 1
        echo "Stopping $SERVICE_NAME..."
        sudo systemctl stop "$SERVICE_NAME"
        sudo systemctl status "$SERVICE_NAME" --no-pager
        ;;
    restart)
        echo "Checking for in-progress jobs before restarting..."
        check_running_jobs || exit 1
        echo "Restarting $SERVICE_NAME..."
        # kill_port_process
        sudo systemctl restart "$SERVICE_NAME"
        sleep 2
        sudo systemctl status "$SERVICE_NAME" --no-pager
        ;;
    status)
        sudo systemctl status "$SERVICE_NAME" --no-pager
        ;;
    logs)
        echo "Showing logs for $SERVICE_NAME (Ctrl+C to exit)..."
        sudo journalctl -u "$SERVICE_NAME" -f
        ;;
    logs-tail)
        echo "Last 50 lines of $SERVICE_NAME logs:"
        sudo journalctl -u "$SERVICE_NAME" -n 50 --no-pager
        ;;
    enable)
        echo "Enabling $SERVICE_NAME to start on boot..."
        sudo systemctl enable "$SERVICE_NAME"
        ;;
    disable)
        echo "Disabling $SERVICE_NAME from starting on boot..."
        sudo systemctl disable "$SERVICE_NAME"
        ;;
    # kill-port)
    #     echo "Killing any process on port $PORT..."
    #     kill_port_process
    #     ;;
    *)
        echo "Bulk Snapshots UI Service Manager"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|logs|logs-tail|enable|disable|kill-port}"
        echo ""
        echo "Commands:"
        echo "  start      - Start the service (kills port conflicts first)"
        echo "  stop       - Stop the service (checks for running jobs first)"
        echo "  restart    - Restart the service (checks for running jobs first)"
        echo "  status     - Show service status"
        echo "  logs       - Follow live logs (Ctrl+C to exit)"
        echo "  logs-tail  - Show last 50 log lines"
        echo "  enable     - Enable service to start on boot"
        echo "  disable    - Disable service from starting on boot"
        echo "  kill-port  - Kill any process using port $PORT"
        exit 1
        ;;
esac
