"""Trigger all data processing pipelines for a specific study"""

import azure.storage.filedatalake as azurelake

import config
from pathlib import Path


def pipeline(study_id: str):
    """Reads the contents of the maestro2 datatype folder. For each modality folder trigger the data processing pipeline. Each folder in this stage is a modality so the modality is passed to the data processing pipeline as an argument."""

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    study_folder = f"{study_id}/pooled-data/Maestro2"

    # Get the container client for the study
    modality_folders = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # Get the list of folders in the study folder
    paths = modality_folders.get_paths(recursive=False, path=study_folder)

    str_paths = [str(path.name) for path in paths]

    for str_path in str_paths:
        # Extract the modality from the path
        modality = Path(str_path).name

        # Trigger the data processing pipeline
        print(f"Triggering data processing pipeline for study: {modality}")

        # Call the data processing pipeline
        # data_processing_pipeline(modality)

    return
