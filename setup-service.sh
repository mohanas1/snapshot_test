#!/bin/bash
# Setup script for bulk-snapshots-ui systemd service

set -e

SERVICE_NAME="bulk-snapshots-ui"
SERVICE_FILE="$SERVICE_NAME.service"
INSTALL_PATH="/etc/systemd/system/$SERVICE_FILE"

echo "========================================="
echo "Bulk Snapshots UI Service Setup"
echo "========================================="
echo ""

# Check if running with sudo/root
if [ "$EUID" -ne 0 ]; then
    echo "This script needs sudo privileges to install the systemd service."
    echo "Re-running with sudo..."
    exec sudo bash "$0" "$@"
fi

# Stop existing service if running
if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "Stopping existing service..."
    systemctl stop "$SERVICE_NAME"
fi

# Copy service file
echo "Installing service file to $INSTALL_PATH..."
cp "$SERVICE_FILE" "$INSTALL_PATH"

# Set correct permissions
chmod 644 "$INSTALL_PATH"

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload

# Enable service to start on boot
echo "Enabling service to start on boot..."
systemctl enable "$SERVICE_NAME"

# Start the service
echo "Starting service..."
systemctl start "$SERVICE_NAME"

# Wait a moment for service to start
sleep 2

# Show status
echo ""
echo "========================================="
echo "Service Status:"
echo "========================================="
systemctl status "$SERVICE_NAME" --no-pager || true

echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Useful commands:"
echo "  View status:   sudo systemctl status $SERVICE_NAME"
echo "  Start:         sudo systemctl start $SERVICE_NAME"
echo "  Stop:          sudo systemctl stop $SERVICE_NAME"
echo "  Restart:       sudo systemctl restart $SERVICE_NAME"
echo "  View logs:     sudo journalctl -u $SERVICE_NAME -f"
echo "  Disable:       sudo systemctl disable $SERVICE_NAME"
echo ""
echo "Application should be running on: http://0.0.0.0:8765"
echo ""
