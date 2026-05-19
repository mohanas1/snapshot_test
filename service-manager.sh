#!/bin/bash
# Quick management script for bulk-snapshots-ui service

SERVICE_NAME="bulk-snapshots-ui"

case "$1" in
    start)
        echo "Starting $SERVICE_NAME..."
        sudo systemctl start "$SERVICE_NAME"
        sudo systemctl status "$SERVICE_NAME" --no-pager
        ;;
    stop)
        echo "Stopping $SERVICE_NAME..."
        sudo systemctl stop "$SERVICE_NAME"
        sudo systemctl status "$SERVICE_NAME" --no-pager
        ;;
    restart)
        echo "Restarting $SERVICE_NAME..."
        sudo systemctl restart "$SERVICE_NAME"
        sleep 1
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
    *)
        echo "Bulk Snapshots UI Service Manager"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|logs|logs-tail|enable|disable}"
        echo ""
        echo "Commands:"
        echo "  start      - Start the service"
        echo "  stop       - Stop the service"
        echo "  restart    - Restart the service"
        echo "  status     - Show service status"
        echo "  logs       - Follow live logs (Ctrl+C to exit)"
        echo "  logs-tail  - Show last 50 log lines"
        echo "  enable     - Enable service to start on boot"
        echo "  disable    - Disable service from starting on boot"
        exit 1
        ;;
esac
