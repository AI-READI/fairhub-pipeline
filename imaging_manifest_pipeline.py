"""Organize the imaging data into CDS format"""

import os
import tempfile
import shutil
import contextlib
import azure.storage.filedatalake as azurelake
import config
import json
import utils.logwatch as logging
from traceback import format_exc
import pandas as pd


def get_json_filenames(folder_path):
    json_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # Check if the file ends with .json and doesn't start with a dot
            if file.endswith(".json") and not file.startswith("."):
                # Append the full path by joining root and file
                full_path = os.path.join(root, file)
                json_files.append(full_path)

    return json_files


def pipeline(
    study_id: str,
):  # sourcery skip: extract-duplicate-method, low-code-quality
    """Organize the imaging data into CDS format
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    # source_imaging_folder = f"{study_id}/completed/imaging"
    # source_metadata_folder = f"{study_id}/completed/imaging-metadata"
    source_imaging_folder = (
        f"{study_id}/pooled-data/imaging-test/test_manifest_creation/imaging"
    )
    source_metadata_folder = (
        f"{study_id}/pooled-data/imaging-test/test_manifest_creation/imaging-metadata"
    )

    destination_folder = f"{study_id}/completed/imaging-manifest"

    logger = logging.Logwatch("drain", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # with contextlib.suppress(Exception):
    #     file_system_client.delete_directory(destination_folder)

    # Create the output folder
    with contextlib.suppress(Exception):
        file_system_client.create_directory(destination_folder)

    imaging_file_paths = []
    metadata_file_paths = []
    mfp = []
    ifp = []

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="imaging_manifest_pipeline_meta_")

    # Get the data file paths
    data_file_count = 0
    paths = file_system_client.get_paths(path=source_imaging_folder, recursive=True)

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        os.path.dirname(t)

        # Check if file_name has an extension (removes folders)
        extension = file_name.split(".")[-1]
        if extension is None:
            logger.debug(f"Skipping {file_name} - Seems to be a folder")
            continue

        # Check if the item is an dicom file
        if not file_name.endswith(".dcm") or file_name.startswith("."):
            continue

        # remove the extension if it exists
        cleaned_file_name = file_name.split(extension)[0]
        formatted_file_name = cleaned_file_name.replace("_", "").replace(".", "")

        data_file_count += 1

        if data_file_count % 1000 == 0:
            logger.trace(
                f"Found at least {data_file_count} files in the {source_imaging_folder} folder"
            )

        imaging_file_paths.append(
            {
                "file_path": t,
                "file_name": file_name,
                "cleaned_file_name": cleaned_file_name,
                "formatted_file_name": formatted_file_name,
            }
        )
        ifp.append(formatted_file_name)

    logger.info(f"Found {data_file_count} files in the {source_imaging_folder} folder")
    logger.info(f"Current total files: {len(imaging_file_paths)}")

    # Get the metadata file paths
    metadata_file_count = 0
    paths = file_system_client.get_paths(path=source_metadata_folder, recursive=True)

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        # Check if file_name has an extension (removes folders)
        extension = file_name.split(".")[-1]
        if extension is None:
            logger.debug(f"Skipping {file_name} - Seems to be a folder")
            continue

        # Check if the item is a json file
        if not file_name.endswith(".json") or file_name.startswith("."):
            continue

        # remove the extension if it exists
        cleaned_file_name = file_name.split(extension)[0]
        formatted_file_name = (
            cleaned_file_name.replace("_dcm", "").replace("_", "").replace(".", "")
        )

        metadata_file_count += 1

        if metadata_file_count % 1000 == 0:
            logger.trace(
                f"Found at least {metadata_file_count} files in the {source_metadata_folder} folder"
            )

        metadata_file_paths.append(
            {
                "file_path": t,
                "file_name": file_name,
                "cleaned_file_name": cleaned_file_name,
                "formatted_file_name": formatted_file_name,
            }
        )
        mfp.append(formatted_file_name)

    logger.info(
        f"Found {metadata_file_count} files in the {source_metadata_folder} folder"
    )
    logger.info(f"Current total files: {len(metadata_file_paths)}")

    if set(sorted(mfp)) == set(sorted(ifp)):
        logger.info("The metadata and imaging files are the same")
    else:
        logger.error("The metadata and imaging files are not the same")

    with tempfile.TemporaryDirectory(
        prefix="imaging_manifest_pipeline_"
    ) as temp_folder_path:
        for idx, file_item in enumerate(metadata_file_paths):
            path = file_item["file_path"]
            file_name = file_item["file_name"]

            cleaned_path = path.split(source_metadata_folder)[1]
            download_path = f"{temp_folder_path}{cleaned_path}"

            parent_folder = os.path.dirname(download_path)
            os.makedirs(parent_folder, exist_ok=True)

            with open(file=download_path, mode="wb") as f:
                f.write(
                    file_system_client.get_file_client(file_path=path)
                    .download_file()
                    .readall()
                )

            logger.debug(
                f"Downloaded {file_name} to {download_path} - ({idx + 1}/{len(metadata_file_paths)})"
            )

        files = get_json_filenames(f"{temp_folder_path}/retinal_photography")
        data = []

        for json_file in files:
            with open(json_file, "r") as file:
                json_data = json.load(file)

                flattened_data = [value for key, value in json_data.items()]

                df = pd.DataFrame(flattened_data)

                df_filtered = df[
                    [
                        "participant_id",
                        "manufacturer",
                        "manufacturers_model_name",
                        "laterality",
                        "anatomic_region",
                        "imaging",
                        "height",
                        "width",
                        "color_channel_dimension",
                        "sop_instance_uid",
                        "filepath",
                    ]
                ]

                data.append(df_filtered)

        # Concatenate all DataFrames in the list into one large DataFrame
        final_df = pd.concat(data, ignore_index=True)
        final_df = final_df.sort_values(
            ["participant_id", "filepath"], ascending=[True, True]
        )

        retinal_photography_manifest_file_path = (
            f"{meta_temp_folder_path}/retinal_photography_manifest.tsv"
        )

        final_df.to_csv(
            retinal_photography_manifest_file_path,
            sep="\t",
            index=False,
        )

        # Upload the manifest file to the destination folder
        output_file_path = f"{destination_folder}/retinal_photography/manifest.tsv"

        try:
            output_file_client = file_system_client.get_file_client(
                file_path=output_file_path
            )

            with open(file=retinal_photography_manifest_file_path, mode="rb") as f:
                output_file_client.upload_data(f, overwrite=True)

                logger.info(f"Uploaded {file_name} to {output_file_path}")
        except Exception:
            logger.error(f"Failed to upload {file_name}")

            error_exception = "".join(format_exc().splitlines())
            logger.error(error_exception)

        # Load the input_op TSV file
        input_df = pd.read_csv(retinal_photography_manifest_file_path, sep="\t")

        files = get_json_filenames(f"{temp_folder_path}/retinal_oct")
        data = []

        for json_file in files:
            with open(json_file, "r") as file:
                json_data = json.load(file)

                flattened_data = [value for key, value in json_data.items()]

                # Convert the flattened data into a DataFrame
                df = pd.DataFrame(flattened_data)

                # Show only specific columns you are interested in: 'participant_id', 'filepath', 'manufacturer'

                # Filter specific columns
                df_filtered = df[
                    [
                        "participant_id",
                        "manufacturer",
                        "manufacturers_model_name",
                        "anatomic_region",
                        "imaging",
                        "laterality",
                        "height",
                        "width",
                        "number_of_frames",
                        "pixel_spacing",
                        "slice_thickness",
                        "sop_instance_uid",
                        "filepath",
                        "reference_retinal_photography_image_instance_uid",
                    ]
                ].copy()

                #  Add the "reference_filepath" by matching "reference_instance_uid" with the "sop_instance_uid" in input_op
                df_filtered.loc[:, "reference_filepath"] = df_filtered[
                    "reference_retinal_photography_image_instance_uid"
                ].map(input_df.set_index("sop_instance_uid")["filepath"])

                df_filtered.rename(
                    columns={
                        "reference_retinal_photography_image_instance_uid": "reference_instance_uid"
                    },
                    inplace=True,
                )

                data.append(df_filtered)

        final_df = pd.concat(data, ignore_index=True)
        final_df = final_df.sort_values(
            ["participant_id", "filepath"], ascending=[True, True]
        )

        retinal_oct_manifest_file_path = (
            f"{meta_temp_folder_path}/retinal_oct_manifest.tsv"
        )

        final_df.to_csv(
            retinal_oct_manifest_file_path,
            sep="\t",
            index=False,
        )

        # Upload the manifest file to the destination folder
        output_file_path = f"{destination_folder}/retinal_oct/manifest.tsv"

        try:
            output_file_client = file_system_client.get_file_client(
                file_path=output_file_path
            )

            with open(file=retinal_oct_manifest_file_path, mode="rb") as f:
                output_file_client.upload_data(f, overwrite=True)

                logger.info(f"Uploaded {file_name} to {output_file_path}")
        except Exception:
            logger.error(f"Failed to upload {file_name}")

            error_exception = "".join(format_exc().splitlines())
            logger.error(error_exception)

        files = get_json_filenames(f"{temp_folder_path}/retinal_flio")
        data = []

        for json_file in files:
            with open(json_file, "r") as file:
                json_data = json.load(file)

                flattened_data = [value for key, value in json_data.items()]

                # Convert the flattened data into a DataFrame
                df = pd.DataFrame(flattened_data)

                # Show only specific columns you are interested in: 'participant_id', 'filepath', 'manufacturer'

                # Filter specific columns
                df_filtered = df[
                    [
                        "participant_id",
                        "manufacturer",
                        "manufacturers_model_name",
                        "laterality",
                        "wavelength",
                        "height",
                        "width",
                        "number_of_frames",
                        "sop_instance_uid",
                        "filepath",
                    ]
                ]

                data.append(df_filtered)

        final_df = pd.concat(data, ignore_index=True)
        final_df = final_df.sort_values(
            ["participant_id", "filepath"], ascending=[True, True]
        )

        retinal_flio_manifest_file_path = (
            f"{meta_temp_folder_path}/retinal_flio_manifest.tsv"
        )

        final_df.to_csv(
            retinal_flio_manifest_file_path,
            sep="\t",
            index=False,
        )

        # Upload the manifest file to the destination folder
        output_file_path = f"{destination_folder}/retinal_flio/manifest.tsv"

        try:
            output_file_client = file_system_client.get_file_client(
                file_path=output_file_path
            )

            with open(file=retinal_flio_manifest_file_path, mode="rb") as f:
                output_file_client.upload_data(f, overwrite=True)

                logger.info(f"Uploaded {file_name} to {output_file_path}")
        except Exception:
            logger.error(f"Failed to upload {file_name}")

            error_exception = "".join(format_exc().splitlines())
            logger.error(error_exception)

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
