"""Trigger all the data processing pipelines for all the studies"""

import azure.storage.filedatalake as azurelake

import config


def pipeline():
    """Reads the contents of the stage-1-container. For each study, triggers the data processing pipeline. Each folder in the stage-1-container is a study. The study_id is the name of the folder. The study_id is passed to the data processing pipeline as an argument."""

    # Get the container client
    study_folders = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    paths = study_folders.get_paths(recursive=False)

    str_paths = [str(path.name) for path in paths]

    for study_id in str_paths:
        # Trigger the data processing pipeline
        print(f"Triggering data processing pipeline for study: {study_id}")

        # Call the data processing pipeline
        # data_processing_pipeline(study_id)

    return
