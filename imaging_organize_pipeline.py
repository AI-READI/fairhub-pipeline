"""Organize the imaging data into CDS format"""

import os
import tempfile
import shutil
import contextlib
import azure.storage.filedatalake as azurelake
import config
import csv
import utils.logwatch as logging
from traceback import format_exc
from utils.time_estimator import TimeEstimator


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Organize the imaging data into CDS format
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    source_data_folders = [
        # f"{study_id}/pooled-data/imaging-test/test/pilot-imaging/Optomed-processed",
        # f"{study_id}/pooled-data/imaging-test/test/pooled-data/Optomed-processed",
        f"{study_id}/pilot-imaging/Optomed-processed",
        f"{study_id}/pilot-imaging/Eidon-processed",
        f"{study_id}/pilot-imaging/Spectralis-processed",
        f"{study_id}/pilot-imaging/Maestro2-processed",
        f"{study_id}/pilot-imaging/Triton-processed",
        f"{study_id}/pilot-imaging/Cirrus-processed",
        f"{study_id}/pilot-imaging/Flio-processed",
        f"{study_id}/pooled-data/Optomed-processed",
        f"{study_id}/pooled-data/Eidon-processed",
        f"{study_id}/pooled-data/Spectralis-processed",
        f"{study_id}/pooled-data/Maestro2-processed",
        f"{study_id}/pooled-data/Triton-processed",
        f"{study_id}/pooled-data/Cirrus-processed",
        f"{study_id}/pooled-data/Flio-processed",
        f"{study_id}/custom-formatted-imaging/Optomed",
        f"{study_id}/custom-formatted-imaging/Eidon",
        f"{study_id}/custom-formatted-imaging/Spectralis",
        f"{study_id}/custom-formatted-imaging/Maestro2",
        f"{study_id}/custom-formatted-imaging/Triton",
        f"{study_id}/custom-formatted-imaging/Cirrus",
        f"{study_id}/custom-formatted-imaging/Flio",
    ]

    source_metadata_folders = [
        # f"{study_id}/pooled-data/imaging-test/test/pilot-imaging/Optomed-metadata",
        # f"{study_id}/pooled-data/imaging-test/test/pooled-data/Optomed-metadata",
        f"{study_id}/pilot-imaging/Optomed-metadata",
        f"{study_id}/pilot-imaging/Eidon-metadata",
        f"{study_id}/pilot-imaging/Spectralis-metadata",
        f"{study_id}/pilot-imaging/Maestro2-metadata",
        f"{study_id}/pilot-imaging/Triton-metadata",
        f"{study_id}/pilot-imaging/Cirrus-metadata",
        f"{study_id}/pilot-imaging/Flio-metadata",
        f"{study_id}/pooled-data/Optomed-metadata",
        f"{study_id}/pooled-data/Eidon-metadata",
        f"{study_id}/pooled-data/Spectralis-metadata",
        f"{study_id}/pooled-data/Maestro2-metadata",
        f"{study_id}/pooled-data/Triton-metadata",
        f"{study_id}/pooled-data/Cirrus-metadata",
        f"{study_id}/pooled-data/Flio-metadata",
        f"{study_id}/custom-formatted-imaging/Optomed-metadata",
        f"{study_id}/custom-formatted-imaging/Eidon-metadata",
        f"{study_id}/custom-formatted-imaging/Spectralis-metadata",
        f"{study_id}/custom-formatted-imaging/Maestro2-metadata",
        f"{study_id}/custom-formatted-imaging/Triton-metadata",
        f"{study_id}/custom-formatted-imaging/Cirrus-metadata",
        f"{study_id}/custom-formatted-imaging/Flio-metadata",
    ]

    data_destination_folder = f"{study_id}/completed/imaging"
    metadata_destination_folder = f"{study_id}/completed/imaging-metadata"

    participant_filter_list_file = f"{study_id}/dependency/PatientID/AllParticipantIDs07-01-2023through05-01-2025.csvcsv"
    ignore_file = (
        f"{study_id}/ignore/imaging/ignore_post_processing_2024_10_02_16_0755.txt"
    )

    logger = logging.Logwatch("drain", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(data_destination_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(metadata_destination_folder)

    data_file_paths = []
    metadata_file_paths = []
    participant_filter_list = []
    ignore_list = []

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="imaging_organize_pipeline_meta_")

    # Get the participant filter list file
    with contextlib.suppress(Exception):
        file_client = file_system_client.get_file_client(
            file_path=participant_filter_list_file
        )

        temp_participant_filter_list_file = os.path.join(
            meta_temp_folder_path, "filter_file.csv"
        )

        with open(file=temp_participant_filter_list_file, mode="wb") as f:
            f.write(file_client.download_file().readall())

        with open(file=temp_participant_filter_list_file, mode="r") as f:
            reader = csv.reader(f)
            for row in reader:
                participant_filter_list.append(row[0])

        # remove the first row
        participant_filter_list.pop(0)

    # Get the ignore list
    with contextlib.suppress(Exception):
        file_client = file_system_client.get_file_client(file_path=ignore_file)

        temp_ignore_list_file = os.path.join(meta_temp_folder_path, "ignore_list.csv")

        with open(file=temp_ignore_list_file, mode="wb") as f:
            f.write(file_client.download_file().readall())

        with open(file=temp_ignore_list_file, mode="r") as f:
            ignore_list = f.readlines()

        # Save trimmed fignore_liste names
        ignore_list = [x.strip() for x in ignore_list]

        # Remove empty lines
        ignore_list = [x for x in ignore_list if x]

        # Remove duplicates
        ignore_list = list(set(ignore_list))

        # Replace any _ or . with "" in the ignore list
        ignore_list = [x.replace("_", "") for x in ignore_list]
        ignore_list = [x.replace(".", "") for x in ignore_list]

    for source_data_folder in source_data_folders:
        paths = file_system_client.get_paths(path=source_data_folder, recursive=True)

        data_file_count = 0

        for path in paths:
            t = str(path.name)

            file_name = t.split("/")[-1]

            # Check if file_name has an extension (removes folders)
            extension = file_name.split(".")[-1]
            if extension is None:
                logger.debug(f"Skipping {file_name} - Seems to be a folder")
                continue

            # Check if the item is an dicom or json file
            if not file_name.endswith(".dcm") and not file_name.endswith(".json"):
                continue

            # remove the extension if it exists
            cleaned_file_name = file_name.split(extension)[0]
            formatted_file_name = cleaned_file_name.replace("_", "").replace(".", "")

            file_ignored = False

            for item in ignore_list:
                if item in formatted_file_name:
                    file_ignored = True
                    break

            if file_ignored:
                logger.debug(f"Skipping {file_name} - Ignored")
                continue

            patient_id = cleaned_file_name.split("_")[0]

            if patient_id not in participant_filter_list:
                logger.debug(
                    f"Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
                )
                continue

            data_file_count += 1

            if data_file_count % 1000 == 0:
                logger.trace(
                    f"Found at least {data_file_count} files in the data folder"
                )

            data_file_paths.append(
                {
                    "file_path": t,
                    "file_name": file_name,
                    "source_folder": source_data_folder,
                }
            )

        logger.info(f"Found {data_file_count} files in the {source_data_folder} folder")
        logger.info(f"Current total files: {len(data_file_paths)}")

    total_files = len(data_file_paths)
    logger.info(f"Found {total_files} items in the data folder")

    time_estimator = TimeEstimator(total_files)

    with tempfile.TemporaryDirectory(
        prefix="imaging_organize_pipeline_"
    ) as temp_folder_path:
        for file_item in data_file_paths:
            path = file_item["file_path"]
            file_name = file_item["file_name"]
            source_folder = file_item["source_folder"]

            download_path = os.path.join(temp_folder_path, file_name)

            with open(file=download_path, mode="wb") as f:
                f.write(
                    file_system_client.get_file_client(file_path=path)
                    .download_file()
                    .readall()
                )

            logger.debug(f"Downloaded {file_name} to {download_path}")

            # Upload the file to the data destination folder
            cleaned_file_path = path.split(source_folder)[-1]
            output_file_path = f"{data_destination_folder}{cleaned_file_path}"

            try:
                output_file_client = file_system_client.get_file_client(
                    file_path=output_file_path
                )

                with open(file=download_path, mode="rb") as f:
                    output_file_client.upload_data(f, overwrite=True)

                    logger.fastInfo(f"Uploaded {file_name} to {output_file_path}")
            except Exception:
                logger.error(f"Failed to upload {file_name}")

                error_exception = "".join(format_exc().splitlines())
                logger.error(error_exception)

                logger.fastTime(time_estimator.step())
                continue

            logger.fastTime(time_estimator.step())
            os.remove(download_path)

    for source_metadata_folder in source_metadata_folders:
        paths = file_system_client.get_paths(
            path=source_metadata_folder, recursive=True
        )

        metadata_file_count = 0

        for path in paths:
            t = str(path.name)

            file_name = t.split("/")[-1]

            # Check if file_name has an extension (removes folders)
            extension = file_name.split(".")[-1]
            if extension is None:
                logger.debug(f"Skipping {file_name} - Seems to be a folder")
                continue

            # Check if the item is an dicom or json file
            if not file_name.endswith(".dcm") and not file_name.endswith(".json"):
                continue

            # remove the extension if it exists
            cleaned_file_name = file_name.split(extension)[0]
            formatted_file_name = cleaned_file_name.replace("_", "").replace(".", "")

            file_ignored = False

            for item in ignore_list:
                if item in formatted_file_name:
                    file_ignored = True
                    break

            if file_ignored:
                logger.debug(f"Skipping {file_name} - Ignored")
                continue

            patient_id = cleaned_file_name.split("_")[0]

            if patient_id not in participant_filter_list:
                logger.debug(
                    f"Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
                )
                continue

            metadata_file_count += 1

            if metadata_file_count % 1000 == 0:
                logger.trace(
                    f"Found at least {metadata_file_count} files in the metadata folder"
                )

            metadata_file_paths.append(
                {
                    "file_path": t,
                    "file_name": file_name,
                    "source_folder": source_metadata_folder,
                }
            )

        logger.info(
            f"Found {metadata_file_count} files in the {source_metadata_folder} folder"
        )
        logger.info(f"Current total files: {len(data_file_paths)}")

    total_files = len(metadata_file_paths)
    logger.info(f"Found {total_files} items in the metadata folder")

    time_estimator = TimeEstimator(total_files)

    with tempfile.TemporaryDirectory(
        prefix="imaging_organize_pipeline_"
    ) as temp_folder_path:
        for file_item in metadata_file_paths:
            path = file_item["file_path"]
            file_name = file_item["file_name"]
            source_folder = file_item["source_folder"]

            download_path = os.path.join(temp_folder_path, file_name)

            with open(file=download_path, mode="wb") as f:
                f.write(
                    file_system_client.get_file_client(file_path=path)
                    .download_file()
                    .readall()
                )

            logger.debug(f"Downloaded {file_name} to {download_path}")

            # Upload the file to the data destination folder
            cleaned_file_path = path.split(source_folder)[-1]
            output_file_path = f"{metadata_destination_folder}{cleaned_file_path}"

            try:
                output_file_client = file_system_client.get_file_client(
                    file_path=output_file_path
                )

                with open(file=download_path, mode="rb") as f:
                    output_file_client.upload_data(f, overwrite=True)

                    logger.fastInfo(f"Uploaded {file_name} to {output_file_path}")
            except Exception:
                logger.error(f"Failed to upload {file_name}")

                error_exception = "".join(format_exc().splitlines())
                logger.error(error_exception)

                logger.fastTime(time_estimator.step())
                continue

            logger.fastTime(time_estimator.step())
            os.remove(download_path)

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
