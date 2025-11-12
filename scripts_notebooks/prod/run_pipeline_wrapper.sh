#!/bin/bash
# Wrapper script for run_pipeline.py to be used with launchd

# Set the project directory
PROJECT_DIR="/Users/davidjcox/Downloads/Mosaic/weekly-supervision-pull"
cd "$PROJECT_DIR" || exit 1

# Activate virtual environment and run the pipeline
source venv/bin/activate
python3 "$PROJECT_DIR/scripts_notebooks/prod/run_pipeline.py"

# Capture exit code
EXIT_CODE=$?

# Deactivate virtual environment
deactivate

exit $EXIT_CODE

