#!/bin/bash
# View VM operations log file

LOG_FILE="/home/mohan.as1/mohan_helpers/bulk_snapshots_ui/logs/vm_operations.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "Log file does not exist yet: $LOG_FILE"
    echo "Try a snapshot or disk operation first to generate logs."
    exit 1
fi

case "$1" in
    tail)
        echo "Showing last 50 lines of VM operations log:"
        echo "=========================================="
        tail -50 "$LOG_FILE"
        ;;
    follow)
        echo "Following VM operations log (Ctrl+C to exit):"
        echo "=========================================="
        tail -f "$LOG_FILE"
        ;;
    all)
        echo "Showing entire VM operations log:"
        echo "=========================================="
        cat "$LOG_FILE"
        ;;
    clear)
        echo "Clearing VM operations log..."
        > "$LOG_FILE"
        echo "Log cleared."
        ;;
    *)
        echo "VM Operations Log Viewer"
        echo ""
        echo "Usage: $0 {tail|follow|all|clear}"
        echo ""
        echo "Commands:"
        echo "  tail    - Show last 50 lines"
        echo "  follow  - Follow log in real-time (Ctrl+C to exit)"
        echo "  all     - Show entire log"
        echo "  clear   - Clear the log file"
        echo ""
        echo "Log location: $LOG_FILE"
        exit 1
        ;;
esac
