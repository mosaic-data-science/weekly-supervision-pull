#!/bin/bash
# Wrapper script for run_pipeline.py to be used with launchd

# Check if current time in ET is 7:30 AM (within 1 minute window)
# This ensures the script only runs at 7:30 AM ET regardless of system timezone
ET_TIME=$(TZ="America/New_York" date +"%H:%M")
ET_HOUR=$(TZ="America/New_York" date +"%H")
ET_MINUTE=$(TZ="America/New_York" date +"%M")

if [ "$ET_HOUR" != "07" ] || [ "$ET_MINUTE" != "30" ]; then
    echo "Current ET time is $ET_TIME, not 7:30 AM. Exiting."
    exit 0
fi

echo "Running at 7:30 AM ET ($ET_TIME)"

# Set the project directory
PROJECT_DIR="/Users/davidjcox/Downloads/Mosaic/daily-supervision-pull"
cd "$PROJECT_DIR" || exit 1

# Activate virtual environment and run the pipeline
source venv/bin/activate

# Use the venv's Python explicitly
PYTHON_VENV="$PROJECT_DIR/venv/bin/python3"

# Run the pipeline
$PYTHON_VENV "$PROJECT_DIR/scripts_notebooks/prod/run_pipeline.py"

# Capture exit code
EXIT_CODE=$?

# Send email notification based on exit code
# Don't fail the wrapper if email sending fails, but log it
echo "Pipeline completed with exit code: $EXIT_CODE"
echo "Sending email notification..."
$PYTHON_VENV "$PROJECT_DIR/scripts_notebooks/prod/send_email.py" $EXIT_CODE || echo "Warning: Email sending failed, but continuing..."

# Deactivate virtual environment
deactivate

exit $EXIT_CODE

