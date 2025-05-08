#!/bin/bash

# Set the directory paths
PROJECT_DIR="/home/azureuser/fairhub-pipeline"  
VENV_DIR="$PROJECT_DIR/.venv"
PYTHON_SCRIPT="$PROJECT_DIR/cgm_pipeline.py"    
ARGS="--workers 8"

# Go to project directory
cd "$PROJECT_DIR" || exit

# Print the current directory
echo "Current directory: $PWD"

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Run the Python script
python3 "$PYTHON_SCRIPT" $ARGS > cgm.log 2>&1

# Deactivate virtual environment
deactivate