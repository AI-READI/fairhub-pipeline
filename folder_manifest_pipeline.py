"""Create a file manifest for the folder"""

import tempfile
import azure.storage.filedatalake as azurelake
import config
import utils.logwatch as logging
from tqdm import tqdm

from utils.time_estimator import TimeEstimator

# import hashlib


def pipeline():  # sourcery skip: low-code-quality
    """Create a file manifest for the folder"""
    FAST_FOLDER_CHECK = True

    source_folder = "/"
    # source_folder = f"{study_id}/completed/cardiac_ecg"

    destination_file = "/s/file-manifest.tsv"
    # destination_file = f"{study_id}/completed/file-manifest.tsv"

    logger = logging.Logwatch("drain", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )

    file_paths = []

    # Get the data file paths
    data_file_count = 0
    paths = file_system_client.get_paths(path=source_folder, recursive=True)

    for path in paths:
        file_path = str(path.name)

        file_name = file_path.split("/")[-1]

        data_file_count += 1

        if data_file_count % 1000 == 0:
            print(
                f"Found at least {data_file_count} files in the {source_folder} folder"
            )

        # Check if file_name has an extension (removes folders)
        if FAST_FOLDER_CHECK:
            length = len(file_name.split("."))
            extension = file_name.split(".")[-1]

            if length == 1 or extension is None:
                # print(f"Skipping {file_name} - Seems to be a folder (FFC)")
                continue

        file_client = file_system_client.get_file_client(file_path=file_path)
        file_properties = file_client.get_file_properties()
        file_metadata = file_properties.metadata

        if file_metadata.get("hdi_isfolder"):
            print(f"Skipping {file_path} - Seems to be a folder (Full)")
            continue

        file_paths.append(
            {
                "file_path": file_path,
                "file_name": file_name,
                "md5_checksum": 0,
            }
        )

    logger.info(f"Found {data_file_count} files in the {source_folder} folder")
    total_files = len(file_paths)

    logger.info(f"Current total files: {total_files}")

    time_estimator = TimeEstimator(total_files)

    # with tempfile.TemporaryDirectory(
    #     prefix="folder_manifest_download_pipeline_"
    # ) as temp_folder_path:
    for file_item in tqdm(file_paths, desc="Processing files"):
        path = file_item["file_path"]
        file_name = file_item["file_name"]

        # download_path = os.path.join(temp_folder_path, file_name)

        # with open(file=download_path, mode="wb") as f:
        #     f.write(
        #         file_system_client.get_file_client(file_path=path)
        #         .download_file()
        #         .readall()
        #     )

        # Calculate the md5 checksum of the file
        # file_item["md5_checksum"] = hashlib.md5(
        #     open(download_path, "rb").read()
        # ).hexdigest()

        # logger.noPrintTrace(f"Downloaded {file_name} to {download_path}")

        file_item["manifest_file_path"] = f"{path}"

        logger.noPrintTime(time_estimator.step())
        # os.remove(download_path)

    with tempfile.TemporaryDirectory(
        prefix="folder_manifest_meta_pipeline_"
    ) as temp_folder_path:
        # Write the manifest file
        manifest_file_path = f"{temp_folder_path}/file-manifest.tsv"

        with open(manifest_file_path, "w") as f:
            f.write("file_name\tmd5_checksum\tfile_path\n")

            for file_item in tqdm(file_paths, desc="Writing manifest"):
                f.write(
                    f"{file_item['file_name']}\t{file_item['md5_checksum']}\t{file_item['manifest_file_path']}\n"
                )

        # Upload the manifest file to the destination folder
        output_file_path = f"{destination_file}"

        with open(file=manifest_file_path, mode="rb") as f:
            output_file_client = file_system_client.get_file_client(
                file_path=output_file_path
            )

            output_file_client.upload_data(f, overwrite=True)

            logger.info(f"Uploaded manifest to {output_file_path}")


if __name__ == "__main__":
    pipeline()
