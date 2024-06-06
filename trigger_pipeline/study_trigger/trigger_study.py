"""Trigger all data processing pipelines for a specific study"""

import azure.storage.filedatalake as azurelake
import requests
import config
from pathlib import Path


def pipeline(study_id: str):
    """Reads the contents of a specific study folder. For each folder trigger the data processing pipeline. Each folder in this stage is a datatype so the datatype is passed to the data processing pipeline as an argument."""

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    study_folder = f"{study_id}/pooled-data"

    # Get the container client for the study
    data_type_folders = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # Get the list of folders in the study folder
    paths = data_type_folders.get_paths(recursive=False, path=study_folder)

    str_paths = [str(path.name) for path in paths]

    for str_path in str_paths:
        # Extract the data type from the path
        data_type = Path(str_path).name

        # Trigger the data processing pipeline
        print(f"Triggering data processing pipeline for study: {data_type}")

        if data_type == "Maestro2":
            # Call the data processing pipeline
            print("Maestro2")

            body = {"study_id": study_id}

            # Call the data processing pipeline
            requests.post(
                f"{config.FAIRHUB_PIPELINE_URL}/process-maestro-2",
                json=body,
                headers={
                    "Authorization": f"Bearer {config.FAIRHUB_ACCESS_TOKEN}",
                    "Content-Type": "application/json",
                },
            )

            # maestro2_pipeline()
        elif data_type == "EnvSensor":
            # Call the data processing pipeline
            print("EnvSensor")
            # env_sensor_pipeline()
        else:
            # We can either ignore this one, raise and error or move it unprocessed.
            print("Unknown data type")

    return
