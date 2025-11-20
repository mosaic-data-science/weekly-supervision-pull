#!/bin/bash
# Wrapper script for run_pipeline.py to be used with launchd
# Launchd handles the scheduling, so we just run the pipeline

# Redirect all output to log files for debugging
exec >> /Users/davidjcox/Documents/Mosaic/daily-supervision-pull/logs/launchd_stdout.log 2>> /Users/davidjcox/Documents/Mosaic/daily-supervision-pull/logs/launchd_stderr.log

echo "=========================================="
echo "Pipeline wrapper started at $(TZ="America/New_York" date)"
echo "Current directory: $(pwd)"
echo "Script path: $0"
echo "=========================================="

# Set the project directory
PROJECT_DIR="/Users/davidjcox/Documents/Mosaic/daily-supervision-pull"
cd "$PROJECT_DIR" || {
    echo "ERROR: Failed to change to project directory: $PROJECT_DIR"
    exit 1
}

# Activate virtual environment and run the pipeline
source venv/bin/activate

# Use the venv's Python explicitly
PYTHON_VENV="$PROJECT_DIR/venv/bin/python3"

# Run the pipeline
echo "Running pipeline..."
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

echo "=========================================="
echo "Pipeline wrapper finished at $(TZ="America/New_York" date)"
echo "=========================================="

exit $EXIT_CODE
