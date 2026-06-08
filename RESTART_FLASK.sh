#!/bin/bash
# DEPRECATED: Use service-manager.sh instead
# This script is kept for backward compatibility only
#
# Recommended usage:
#   bash service-manager.sh restart
#   bash service-manager.sh start
#   bash service-manager.sh stop
#   bash service-manager.sh status

echo "⚠️  DEPRECATED: This script is deprecated."
echo "📌 Please use: bash service-manager.sh restart"
echo ""
echo "Redirecting to service-manager.sh in 3 seconds..."
sleep 3

cd /home/mohan.as1/mohan_helpers
exec bash bulk_snapshots_ui/service-manager.sh restart
