#!/bin/bash
# Setup script for daily supervision pull launchd job

PLIST_NAME="com.mosaictherapy.daily-supervision-pull.plist"
PROJECT_DIR="/Users/davidjcox/Downloads/Mosaic/weekly-supervision-pull"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_SOURCE="$PROJECT_DIR/$PLIST_NAME"
PLIST_DEST="$LAUNCH_AGENTS_DIR/$PLIST_NAME"

echo "Setting up launchd job for daily supervision pull..."
echo ""

# Check if plist file exists
if [ ! -f "$PLIST_SOURCE" ]; then
    echo "Error: Plist file not found at $PLIST_SOURCE"
    exit 1
fi

# Create LaunchAgents directory if it doesn't exist
mkdir -p "$LAUNCH_AGENTS_DIR"

# Copy plist to LaunchAgents directory
echo "Copying plist to LaunchAgents directory..."
cp "$PLIST_SOURCE" "$PLIST_DEST"

# Unload existing job if it exists
if launchctl list | grep -q "$PLIST_NAME"; then
    echo "Unloading existing job..."
    launchctl unload "$PLIST_DEST" 2>/dev/null || true
fi

# Load the job
echo "Loading launchd job..."
launchctl load "$PLIST_DEST"

# Check if it loaded successfully
if launchctl list | grep -q "$PLIST_NAME"; then
    echo ""
    echo "✓ Successfully installed and loaded launchd job!"
    echo ""
    echo "The job will run every day at 7:30 AM ET."
    echo ""
    echo "To check job status:"
    echo "  launchctl list | grep $PLIST_NAME"
    echo ""
    echo "To unload the job:"
    echo "  launchctl unload $PLIST_DEST"
    echo ""
    echo "To reload after making changes:"
    echo "  launchctl unload $PLIST_DEST && launchctl load $PLIST_DEST"
    echo ""
    echo "Logs will be written to:"
    echo "  $PROJECT_DIR/scripts_notebooks/prod/logs/launchd_stdout.log"
    echo "  $PROJECT_DIR/scripts_notebooks/prod/logs/launchd_stderr.log"
else
    echo ""
    echo "Error: Failed to load launchd job. Check the plist file for errors."
    exit 1
fi

