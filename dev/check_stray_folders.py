"""Check for stray folders that don't match participant IDs"""

import csv
import os
import tempfile
import azure.storage.filedatalake as azurelake
import config
import utils.logwatch as logging


def process_source_folder(
    source_folder,
    idx,
    total_folders,
    file_system_client,
    participant_ids,
):
    """Process a single source folder and check if folder names match participant IDs"""
    print(f"\n[{idx}/{total_folders}] Processing source folder: {source_folder}")

    # Get the list of folders in the source folder (non-recursive, immediate level only)
    folder_paths = file_system_client.get_paths(path=source_folder, recursive=False)

    stray_folders = []

    for folder_path in folder_paths:
        # Extract folder name from the path
        # folder_path.name is the full path, we need just the folder name
        folder_name = str(folder_path.name).split("/")[-1]

        # Check if folder name is in the participant IDs list
        if folder_name not in participant_ids:
            stray_folder_info = {
                "source_folder": source_folder,
                "folder_path": folder_path.name,
                "folder_name": folder_name,
            }
            stray_folders.append(stray_folder_info)
            print(f"  ⚠️  Stray folder found: {folder_name} (path: {folder_path.name})")

    print(f"Found {len(stray_folders)} stray folder(s) in {source_folder}")
    return stray_folders


def process_manifest_file(
    manifest_file_path,
    idx,
    total_files,
    file_system_client,
    participant_ids,
):
    """Process a single manifest file and check if person_id values match participant IDs"""
    print(f"\n[{idx}/{total_files}] Processing manifest file: {manifest_file_path}")

    stray_person_ids = []

    temp_file_path = None
    try:
        # Download the manifest file from Azure Data Lake to a temporary file
        file_client = file_system_client.get_file_client(file_path=manifest_file_path)
        download = file_client.download_file()

        # Create a temporary file to store the downloaded content
        with tempfile.NamedTemporaryFile(
            mode="w+b", delete=False, suffix=".tsv"
        ) as temp_file:
            temp_file_path = temp_file.name
            # Write the downloaded content to the temporary file
            file_content = download.readall()
            temp_file.write(file_content)

        # Read and parse the TSV content from the temporary file
        with open(temp_file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")

            # Check if person_id column exists
            if "person_id" not in reader.fieldnames:
                print(
                    f"  ⚠️  Warning: 'person_id' column not found in {manifest_file_path}"
                )
                print(f"     Available columns: {', '.join(reader.fieldnames)}")
                # Clean up temporary file
                os.unlink(temp_file_path)
                return stray_person_ids

            # Track unique person_ids to avoid duplicates in report
            seen_person_ids = set()

            for row in reader:
                person_id = row.get("person_id", "").strip()
                if person_id and person_id not in seen_person_ids:
                    seen_person_ids.add(person_id)
                    # Check if person_id is in the participant IDs list

                    if person_id not in participant_ids:
                        stray_person_info = {
                            "manifest_file": manifest_file_path,
                            "person_id": person_id,
                        }
                        stray_person_ids.append(stray_person_info)
                        print(f"  ⚠️  Stray person_id found: {person_id}")

        # Clean up temporary file
        os.unlink(temp_file_path)

    except Exception as exc:
        print(f"  ❌ Error processing manifest file {manifest_file_path}: {exc}")
        # Try to clean up temporary file if it exists
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception:
                pass
        return stray_person_ids

    print(f"Found {len(stray_person_ids)} stray person_id(s) in {manifest_file_path}")
    return stray_person_ids


def pipeline():  # sourcery skip: low-code-quality
    """Check for stray folders that don't match participant IDs"""
    print("=" * 80)
    print("Starting stray folder check pipeline")
    print("=" * 80)

    # Read participant IDs from CSV file
    csv_file_path = r"C:\Users\sanjay\Developer\fairhub-pipeline\ID_only_Participants for Data Release 3 through 05-01-2025.csv"
    print(f"\nReading participant IDs from: {csv_file_path}")

    participant_ids = set()
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        for row in reader:
            if row:  # Skip empty rows
                participant_ids.add(row[0].strip())

    print(f"✓ Loaded {len(participant_ids)} participant IDs")
    print("-" * 80)

    # Define source folders to search for participant data
    # These folders represent different data types and instruments
    source_folders = [
        "YR3/retinal_oct/structural_oct/heidelberg_spectralis",
        "YR3/retinal_octa/enface/heidelberg_spectralis",
        "YR3/retinal_octa/flow_cube/heidelberg_spectralis",
        "YR3/retinal_octa/segmentation/heidelberg_spectralis",
        "YR3/retinal_photography/ir/heidelberg_spectralis",
    ]

    source_files = [
        "YR3/retinal_oct/manifest.tsv",
        "YR3/retinal_octa/manifest.tsv",
        "YR3/retinal_photography/manifest.tsv",
    ]

    print(f"Total source folders to process: {len(source_folders)}")
    print(f"Total manifest files to process: {len(source_files)}")
    print("-" * 80)

    # Initialize logger for tracking operations
    logger = logging.Logwatch("drain", print=True)

    # Connect to Azure Data Lake Storage
    # This will be used to access files in the stage-final-container
    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )
    print("✓ Connected to Azure Data Lake Storage successfully")
    print("-" * 80)

    # List to store all stray folders and person_ids found
    stray_folders = []
    stray_person_ids = []

    # Process source folders sequentially
    print(f"\nProcessing {len(source_folders)} source folders...")

    for idx, source_folder in enumerate(source_folders, 1):
        try:
            folder_stray_folders = process_source_folder(
                source_folder,
                idx,
                len(source_folders),
                file_system_client,
                participant_ids,
            )
            stray_folders.extend(folder_stray_folders)
        except Exception as exc:
            print(f"Source folder {source_folder} generated an exception: {exc}")

    # Process manifest files sequentially
    print(f"\nProcessing {len(source_files)} manifest files...")

    for idx, manifest_file in enumerate(source_files, 1):
        try:
            manifest_stray_person_ids = process_manifest_file(
                manifest_file,
                idx,
                len(source_files),
                file_system_client,
                participant_ids,
            )
            stray_person_ids.extend(manifest_stray_person_ids)
        except Exception as exc:
            print(f"Manifest file {manifest_file} generated an exception: {exc}")

    # Count total stray items found
    total_stray_folders = len(stray_folders)
    total_stray_person_ids = len(stray_person_ids)
    print("\n" + "=" * 80)
    print(f"Stray folder check complete: Found {total_stray_folders} stray folder(s)")
    print(
        f"Stray person_id check complete: Found {total_stray_person_ids} stray person_id(s)"
    )
    print("=" * 80)
    logger.info(f"Total stray folders found: {total_stray_folders}")
    logger.info(f"Total stray person_ids found: {total_stray_person_ids}")

    # Write the stray folders report to disk
    if total_stray_folders > 0:
        print("\nWriting stray folders report to 'stray_folders_report.tsv'...")
        report_filename = "stray_folders_report.tsv"
        with open(report_filename, "w", encoding="utf-8") as f:
            # Write TSV header
            f.write("source_folder\tfolder_path\tfolder_name\n")

            # Write each stray folder entry
            for stray_folder in stray_folders:
                f.write(
                    f"{stray_folder['source_folder']}\t"
                    f"{stray_folder['folder_path']}\t"
                    f"{stray_folder['folder_name']}\n"
                )

        print(f"✓ Stray folders report written successfully: {report_filename}")
        print(f"  Total entries: {total_stray_folders}")
    else:
        print("\n✓ No stray folders found! All folders match participant IDs.")

    # Write the stray person_ids report to disk
    if total_stray_person_ids > 0:
        print("\nWriting stray person_ids report to 'stray_person_ids_report.tsv'...")
        report_filename = "stray_person_ids_report.tsv"
        with open(report_filename, "w", encoding="utf-8") as f:
            # Write TSV header
            f.write("manifest_file\tperson_id\n")

            # Write each stray person_id entry
            for stray_person in stray_person_ids:
                f.write(
                    f"{stray_person['manifest_file']}\t"
                    f"{stray_person['person_id']}\n"
                )

        print(f"✓ Stray person_ids report written successfully: {report_filename}")
        print(f"  Total entries: {total_stray_person_ids}")
    else:
        print(
            "\n✓ No stray person_ids found! All person_ids in manifest files match participant IDs."
        )

    print("=" * 80)
    print("Pipeline completed successfully!")
    print("=" * 80)


# Main entry point: run the pipeline when script is executed directly
if __name__ == "__main__":
    pipeline()
