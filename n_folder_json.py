"""Generate a manifest for a folder"""

import time
import os
import tempfile
import azure.storage.filedatalake as azurelake
import config
import json
import utils.logwatch as logging
from math import log
from utils.time_estimator import TimeEstimator


def prettier_size(n, pow=0, b=1024, u="B", pre=[""] + [f"{p}i" for p in "KMGTPEZY"]):
    r, f = min(int(log(max(n * b**pow, 1), b)), len(pre) - 1), "{:,.%if} %s%s"
    return (f % (abs(r % (-r - 1)), pre[r], u)).format(n * b**pow / b ** float(r))


def pipeline(
    study_id: str,
):  # sourcery skip: extract-duplicate-method, low-code-quality
    """Generate a manifest for a folder
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    FAST_FOLDER_CHECK = True

    source_folders = [
        f"{study_id}/completed/imaging_oct3",
        f"{study_id}/completed/imaging_oct2",
        f"{study_id}/completed/imaging",
    ]

    # source_folders = [
    #     f"{study_id}/pooled-data/imaging-test/test_manifest_creation/imaging",
    # ]

    logger = logging.Logwatch("drain", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    time_checkpoints = TimeEstimator(0)

    for source_folder in source_folders:
        timestr = time.strftime("%Y%m%d-%H%M%S")
        json_file_name = f"file_manifest_{timestr}.json"

        destination_file = f"{study_id}/n/{json_file_name}"

        info = {
            "source_folder": source_folder,
            "destination_file": destination_file,
        }

        file_paths = []

        # Get the data file paths
        data_file_count = 0
        paths = file_system_client.get_paths(path=source_folder, recursive=True)

        for path in paths:
            t = str(path.name)

            relative_file_path = t.split(source_folder)[1]
            relative_file_path = relative_file_path.lstrip("/")

            file_name = t.split("/")[-1]

            data_file_count += 1

            if data_file_count % 1000 == 0:
                logger.debug(
                    f"Found at least {data_file_count} files in the {source_folder} folder"
                )

            # Check if file_name has an extension (removes folders)
            if FAST_FOLDER_CHECK:
                extension = file_name.split(".")[-1]

                if extension is None:
                    logger.noPrintTrace(
                        f"Skipping {file_name} - Seems to be a folder (FFC)"
                    )
                    continue

            file_client = file_system_client.get_file_client(file_path=path)
            file_properties = file_client.get_file_properties()
            file_metadata = file_properties.metadata

            if file_metadata.get("hdi_isfolder"):
                logger.noPrintTrace(f"Skipping {t} - Seems to be a folder")
                continue

            # Get size of file
            size = file_properties.get("size")

            file_paths.append(
                {
                    "file_path": t,
                    "relative_file_path": relative_file_path,
                    "file_name": file_name,
                    "size": size,
                    "size_formatted": prettier_size(size),
                }
            )

        time_checkpoints.split(
            {
                "id": f"read-{source_folder}",
                "description": "Reading the paths from the source folder",
            }
        )

        logger.info(f"Found {data_file_count} files in the {source_folder} folder")
        logger.info(f"Current total files: {len(file_paths)}")

        with tempfile.TemporaryDirectory(prefix="n_folder_json_") as temp_folder_path:
            json_file_path = os.path.join(temp_folder_path, json_file_name)

            j = {
                "info": info,
                "structure": file_paths,
            }

            with open(json_file_path, "w") as file:
                json.dump(j, file, indent=4, sort_keys=True, default=str)

            with open(json_file_path, "rb") as data:
                output_file_client = file_system_client.get_file_client(
                    file_path=destination_file
                )

                output_file_client.upload_data(data, overwrite=True)

            logger.info(f"Uploaded {json_file_name} to {destination_file}")

        time_checkpoints.split(
            {
                "id": f"upload-{source_folder}",
                "description": "Uploading the manifest file",
            }
        )

    for interval in time_checkpoints.intervals():
        logger.time(interval["text"])


if __name__ == "__main__":
    pipeline("AI-READI")
