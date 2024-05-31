"""Trigger all data processing pipelines for a specific study"""

import azure.storage.filedatalake as azurelake

import config


def pipeline():
    """Reads the contents of a specific study folder. For each folder trigger the data processing pipeline. Each folder in this stage is a datatype so the datatype is passed to the data processing pipeline as an argument."""

    study_folder = "AI-READI"

    # Get the container client for the study
    data_type_folders = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # Get the list of folders in the study folder
    paths = data_type_folders.get_paths(recursive=False, path=study_folder)

    str_paths = [str(path.name) for path in paths]

    for data_type in str_paths:
        # Trigger the data processing pipeline
        print(f"Triggering data processing pipeline for study: {data_type}")

        # Call the data processing pipeline
        # data_processing_pipeline(data_type)

    return
