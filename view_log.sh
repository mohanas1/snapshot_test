#!/bin/bash
# View the single consolidated log file for entire Bulk Snapshots UI

LOG_FILE="/home/mohan.as1/mohan_helpers/bulk_snapshots_ui/logs/bulk_snapshots_ui.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "Log file does not exist yet: $LOG_FILE"
    echo "The application may not have started yet."
    exit 1
fi

case "$1" in
    tail)
        LINES="${2:-100}"
        echo "Showing last $LINES lines:"
        echo "=========================================="
        tail -n "$LINES" "$LOG_FILE"
        ;;
    follow)
        echo "Following log in real-time (Ctrl+C to exit):"
        echo "=========================================="
        tail -f "$LOG_FILE"
        ;;
    all)
        echo "Showing entire log:"
        echo "=========================================="
        cat "$LOG_FILE"
        ;;
    search)
        if [ -z "$2" ]; then
            echo "Usage: $0 search <pattern>"
            exit 1
        fi
        LINES="${3:-100}"
        echo "Searching for: $2 (last $LINES matches)"
        echo "=========================================="
        grep -i "$2" "$LOG_FILE" | tail -n "$LINES"
        ;;
    errors)
        LINES="${2:-50}"
        echo "Showing last $LINES ERROR messages:"
        echo "=========================================="
        grep "ERROR" "$LOG_FILE" | tail -n "$LINES"
        ;;
    warnings)
        LINES="${2:-50}"
        echo "Showing last $LINES WARNING messages:"
        echo "=========================================="
        grep "WARNING" "$LOG_FILE" | tail -n "$LINES"
        ;;
    issues)
        LINES="${2:-50}"
        echo "Showing last $LINES ERROR and WARNING messages:"
        echo "=========================================="
        grep -E "ERROR|WARNING" "$LOG_FILE" | tail -n "$LINES"
        ;;
    requests)
        LINES="${2:-50}"
        echo "Showing last $LINES HTTP requests:"
        echo "=========================================="
        grep "REQUEST:" "$LOG_FILE" | tail -n "$LINES"
        ;;
    responses)
        LINES="${2:-50}"
        echo "Showing last $LINES HTTP responses:"
        echo "=========================================="
        grep "RESPONSE:" "$LOG_FILE" | tail -n "$LINES"
        ;;
    api)
        LINES="${2:-50}"
        echo "Showing last $LINES API calls:"
        echo "=========================================="
        grep -E "REQUEST:|RESPONSE:|API|snapshot|disk" "$LOG_FILE" | tail -n "$LINES"
        ;;
    clear)
        echo "Clearing log file..."
        > "$LOG_FILE"
        echo "Log cleared."
        ;;
    size)
        echo "Log file information:"
        echo "=========================================="
        ls -lh "$LOG_FILE"
        echo ""
        echo "Line count: $(wc -l < "$LOG_FILE")"
        echo "Size: $(du -h "$LOG_FILE" | cut -f1)"
        ;;
    stats)
        echo "Log Statistics:"
        echo "=========================================="
        echo "Total lines: $(wc -l < "$LOG_FILE")"
        echo "INFO messages: $(grep -c "INFO" "$LOG_FILE" 2>/dev/null || echo 0)"
        echo "DEBUG messages: $(grep -c "DEBUG" "$LOG_FILE" 2>/dev/null || echo 0)"
        echo "WARNING messages: $(grep -c "WARNING" "$LOG_FILE" 2>/dev/null || echo 0)"
        echo "ERROR messages: $(grep -c "ERROR" "$LOG_FILE" 2>/dev/null || echo 0)"
        echo "HTTP requests: $(grep -c "REQUEST:" "$LOG_FILE" 2>/dev/null || echo 0)"
        echo "HTTP responses: $(grep -c "RESPONSE:" "$LOG_FILE" 2>/dev/null || echo 0)"
        echo ""
        ls -lh "$LOG_FILE"
        ;;
    *)
        echo "======================================================================"
        echo "  BULK SNAPSHOTS UI - Single Consolidated Log Viewer"
        echo "======================================================================"
        echo ""
        echo "Usage: $0 {command} [options]"
        echo ""
        echo "Commands:"
        echo "  tail [N]        - Show last N lines (default: 100)"
        echo "  follow          - Follow log in real-time (Ctrl+C to exit)"
        echo "  all             - Show entire log"
        echo "  search <text> [N] - Search for text, show last N matches (default: 100)"
        echo "  errors [N]      - Show last N ERROR messages (default: 50)"
        echo "  warnings [N]    - Show last N WARNING messages (default: 50)"
        echo "  issues [N]      - Show last N ERROR and WARNING messages (default: 50)"
        echo "  requests [N]    - Show last N HTTP requests (default: 50)"
        echo "  responses [N]   - Show last N HTTP responses (default: 50)"
        echo "  api [N]         - Show last N API-related logs (default: 50)"
        echo "  clear           - Clear the log file"
        echo "  size            - Show log file size and line count"
        echo "  stats           - Show log statistics"
        echo ""
        echo "Examples:"
        echo "  $0 tail 200              # Show last 200 lines"
        echo "  $0 follow                # Follow log in real-time"
        echo "  $0 search snapshot 50    # Search for 'snapshot', show 50 results"
        echo "  $0 search '10.114.55'    # Search for IP address"
        echo "  $0 errors 100            # Show last 100 errors"
        echo "  $0 api                   # Show API-related logs"
        echo "  $0 stats                 # Show statistics"
        echo ""
        echo "Log file: $LOG_FILE"
        echo "======================================================================"
        exit 1
        ;;
esac
