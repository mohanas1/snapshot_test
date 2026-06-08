#!/bin/bash
# View complete application log file

LOG_FILE="/home/mohan.as1/mohan_helpers/bulk_snapshots_ui/logs/bulk_snapshots_app.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "Log file does not exist yet: $LOG_FILE"
    echo "The application may not have started yet."
    exit 1
fi

case "$1" in
    tail)
        LINES="${2:-100}"
        echo "Showing last $LINES lines of application log:"
        echo "=========================================="
        tail -n "$LINES" "$LOG_FILE"
        ;;
    follow)
        echo "Following application log in real-time (Ctrl+C to exit):"
        echo "=========================================="
        tail -f "$LOG_FILE"
        ;;
    all)
        echo "Showing entire application log:"
        echo "=========================================="
        cat "$LOG_FILE"
        ;;
    search)
        if [ -z "$2" ]; then
            echo "Usage: $0 search <pattern>"
            exit 1
        fi
        echo "Searching for: $2"
        echo "=========================================="
        grep -i "$2" "$LOG_FILE" | tail -50
        ;;
    errors)
        echo "Showing ERROR and WARNING messages:"
        echo "=========================================="
        grep -E "ERROR|WARNING" "$LOG_FILE" | tail -50
        ;;
    clear)
        echo "Clearing application log..."
        > "$LOG_FILE"
        echo "Log cleared."
        ;;
    size)
        echo "Log file size:"
        ls -lh "$LOG_FILE"
        echo ""
        echo "Line count:"
        wc -l "$LOG_FILE"
        ;;
    *)
        echo "Bulk Snapshots UI - Complete Application Log Viewer"
        echo ""
        echo "Usage: $0 {tail|follow|all|search|errors|clear|size} [options]"
        echo ""
        echo "Commands:"
        echo "  tail [N]      - Show last N lines (default: 100)"
        echo "  follow        - Follow log in real-time (Ctrl+C to exit)"
        echo "  all           - Show entire log"
        echo "  search <text> - Search for text in log"
        echo "  errors        - Show only ERROR and WARNING messages"
        echo "  clear         - Clear the log file"
        echo "  size          - Show log file size and line count"
        echo ""
        echo "Examples:"
        echo "  $0 tail 200              # Show last 200 lines"
        echo "  $0 search snapshot       # Search for 'snapshot'"
        echo "  $0 search '10.114.55'    # Search for IP address"
        echo ""
        echo "Log location: $LOG_FILE"
        exit 1
        ;;
esac
