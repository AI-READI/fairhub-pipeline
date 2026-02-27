"""Check for stray folders that don't match participant IDs"""

import csv
import os
import tempfile
from collections import defaultdict
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
    """Process a single source folder: find stray folder names (not on ID list) and missing IDs (on list but no folder)."""
    print(f"\n[{idx}/{total_folders}] Processing source folder: {source_folder}")

    # Get the list of folders in the source folder (non-recursive, immediate level only)
    folder_paths = file_system_client.get_paths(path=source_folder, recursive=False)

    # Folder names present in this source folder (patient_id as name)
    folder_names_in_source = set()
    stray_folders = []

    for folder_path in folder_paths:
        folder_name = str(folder_path.name).split("/")[-1]
        folder_names_in_source.add(folder_name)

        if folder_name not in participant_ids:
            stray_folder_info = {
                "source_folder": source_folder,
                "folder_path": folder_path.name,
                "folder_name": folder_name,
            }
            stray_folders.append(stray_folder_info)
            print(f"  ⚠️  Stray folder found: {folder_name} (path: {folder_path.name})")

    # IDs that are on the list but have no folder in this source
    missing_ids = sorted(participant_ids - folder_names_in_source)

    if missing_ids:
        print(f"  📋 Missing IDs (on list but no folder): {len(missing_ids)}")
    print(
        f"Found {len(stray_folders)} stray folder(s), {len(missing_ids)} missing ID(s) in {source_folder}"
    )
    return stray_folders, missing_ids


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
    csv_file_path = r"C:\Users\sanjay\Developer\fairhub-pipeline\year3_person_ids.csv"
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
        # "AI-READI/year3+/cgm/CGM-processed/wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/",
        # "AI-READI/year3+/cirrus/step4_final_structure/retinal_oct/structural_oct/zeiss_cirrus/",
        # "AI-READI/year3+/cirrus/step4_final_structure/retinal_octa/enface/zeiss_cirrus/",
        # "AI-READI/year3+/cirrus/step4_final_structure/retinal_octa/flow_cube/zeiss_cirrus/",
        # "AI-READI/year3+/cirrus/step4_final_structure/retinal_octa/segmentation/zeiss_cirrus/",
        # "AI-READI/year3+/cirrus/step4_final_structure/retinal_photography/ir/zeiss_cirrus/",
        # "AI-READI/year3+/eidon/step4_final_structure/retinal_photography/cfp/icare_eidon/",
        # "AI-READI/year3+/eidon/step4_final_structure/retinal_photography/faf/icare_eidon/",
        # "AI-READI/year3+/eidon/step4_final_structure/retinal_photography/ir/icare_eidon/",
        # "AI-READI/year3+/flio/step5_final_structure/retinal_flio/flio/heidelberg_flio/",
        # "AI-READI/year3+/garmin/Garmin-processed/heart_rate/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/oxygen_saturation/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/physical_activity/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/physical_activity_calorie/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/respiratory_rate/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/sleep/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/stress/garmin_vivosmart5/",
        # "AI-READI/year3+/maestro2/step4_final_structure/retinal_oct/structural_oct/topcon_maestro2/",
        # "AI-READI/year3+/maestro2/step4_final_structure/retinal_octa/enface/topcon_maestro2/",
        # "AI-READI/year3+/maestro2/step4_final_structure/retinal_octa/flow_cube/topcon_maestro2/",
        # "AI-READI/year3+/maestro2/step4_final_structure/retinal_octa/segmentation/topcon_maestro2/",
        # "AI-READI/year3+/maestro2/step4_final_structure/retinal_photography/cfp/topcon_maestro2/",
        # "AI-READI/year3+/maestro2/step4_final_structure/retinal_photography/ir/topcon_maestro2/",
        "AI-READI/year3+/optomed/step4_final_structure/retinal_photography/cfp/optomed_aurora/",
        # "AI-READI/year3+/spectralis-n/step4_final_structure/retinal_oct/structural_oct/heidelberg_spectralis/",
        # "AI-READI/year3+/spectralis-n/step4_final_structure/retinal_photography/ir/heidelberg_spectralis/",
        # "AI-READI/year3+/spectralis-s/retinal_oct/structural_oct/heidelberg_spectralis/",
        # "AI-READI/year3+/spectralis-s/retinal_octa/enface/heidelberg_spectralis/",
        # "AI-READI/year3+/spectralis-s/retinal_octa/flow_cube/heidelberg_spectralis/",
        # "AI-READI/year3+/spectralis-s/retinal_octa/segmentation/heidelberg_spectralis/",
        # "AI-READI/year3+/spectralis-s/retinal_photography/ir/heidelberg_spectralis/",
        # "AI-READI/year3+/triton/step4_final_structure/retinal_oct/structural_oct/topcon_triton/",
        # "AI-READI/year3+/triton/step4_final_structure/retinal_octa/enface/topcon_triton/",
        # "AI-READI/year3+/triton/step4_final_structure/retinal_octa/flow_cube/topcon_triton/",
        # "AI-READI/year3+/triton/step4_final_structure/retinal_octa/segmentation/topcon_triton/",
        # "AI-READI/year3+/triton/step4_final_structure/retinal_photography/cfp/topcon_triton/",
    ]

    source_files = [
        #     "YR3/retinal_oct/manifest.tsv",
        #     "YR3/retinal_octa/manifest.tsv",
        #     "YR3/retinal_photography/manifest.tsv",
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
        file_system_name="stage-one",
    )
    print("✓ Connected to Azure Data Lake Storage successfully")
    print("-" * 80)

    # List to store all stray folders, missing IDs per source, and stray person_ids
    stray_folders = []
    missing_ids_per_source = []  # list of {"source_folder": ..., "participant_id": ...}
    stray_person_ids = []

    # Process source folders sequentially
    print(f"\nProcessing {len(source_folders)} source folders...")

    for idx, source_folder in enumerate(source_folders, 1):
        try:
            folder_stray_folders, folder_missing_ids = process_source_folder(
                source_folder,
                idx,
                len(source_folders),
                file_system_client,
                participant_ids,
            )
            stray_folders.extend(folder_stray_folders)
            missing_ids_per_source.extend(
                [
                    {"source_folder": source_folder, "participant_id": pid}
                    for pid in folder_missing_ids
                ]
            )
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

    # Count total stray items and missing IDs
    total_stray_folders = len(stray_folders)
    total_missing_ids = len(missing_ids_per_source)
    total_stray_person_ids = len(stray_person_ids)
    print("\n" + "=" * 80)
    print(f"Stray folder check complete: Found {total_stray_folders} stray folder(s)")
    print(
        f"Missing ID check complete: Found {total_missing_ids} missing ID(s) (on list but no folder)"
    )
    print(
        f"Stray person_id check complete: Found {total_stray_person_ids} stray person_id(s)"
    )
    print("=" * 80)
    logger.info(f"Total stray folders found: {total_stray_folders}")
    logger.info(f"Total missing IDs (per source folder): {total_missing_ids}")
    logger.info(f"Total stray person_ids found: {total_stray_person_ids}")

    seen_folder_names = set()
    unique_stray_folders = []
    for stray_folder in stray_folders:
        folder_name = stray_folder["folder_name"]
        if folder_name not in seen_folder_names:
            seen_folder_names.add(folder_name)
            unique_stray_folders.append(stray_folder.get("folder_name"))
    total_unique_stray_folders = len(unique_stray_folders)

    print(f"Total unique stray folders: {unique_stray_folders}")

    # Group by source_folder for report: missing IDs and stray folders per source
    missing_by_source = defaultdict(list)
    for entry in missing_ids_per_source:
        missing_by_source[entry["source_folder"]].append(entry["participant_id"])
    stray_by_source = defaultdict(list)
    for stray_folder in stray_folders:
        stray_by_source[stray_folder["source_folder"]].append(
            {
                "folder_path": stray_folder["folder_path"],
                "folder_name": stray_folder["folder_name"],
            }
        )

    # Write combined report: grouped by source_folder, with type (MISSING_ID / STRAY_FOLDER)
    report_filename = "missing_ids_report.txt"
    print(f"\nWriting report to '{report_filename}' (by source_folder, with type)...")
    with open(report_filename, "w", encoding="utf-8") as f:
        f.write("Stray folder check report – by source_folder and type\n")
        f.write("=" * 80 + "\n\n")
        for source_folder in source_folders:
            missing_list = sorted(missing_by_source.get(source_folder, []))
            stray_list = stray_by_source.get(source_folder, [])
            if not missing_list and not stray_list:
                continue
            f.write(f"SOURCE_FOLDER: {source_folder}\n")
            f.write("-" * 80 + "\n")
            # Type: MISSING_ID – on participant list but no folder in this source
            f.write(
                "  Type: MISSING_ID (on participant list but no folder in this source)\n"
            )
            if missing_list:
                f.write(f"  Count: {len(missing_list)}\n")
                # Write IDs in rows of 10 for readability
                for i in range(0, len(missing_list), 10):
                    chunk = missing_list[i : i + 10]
                    f.write("    " + ", ".join(str(x) for x in chunk) + "\n")
            else:
                f.write("  Count: 0\n")
                f.write("    (none)\n")
            f.write("\n")
            # Type: STRAY_FOLDER – folder in source but name not on participant list
            f.write(
                "  Type: STRAY_FOLDER (folder in source but name not on participant list)\n"
            )
            if stray_list:
                f.write(f"  Count: {len(stray_list)}\n")
                for item in stray_list:
                    f.write(f"    folder_name: {item['folder_name']}\n")
                    f.write(f"    folder_path: {item['folder_path']}\n")
            else:
                f.write("  Count: 0\n")
                f.write("    (none)\n")
            f.write("\n")
        f.write("=" * 80 + "\n")
        f.write("End of report\n")
    print(f"✓ Report written successfully: {report_filename}")

    # Write the stray person_ids report to disk
    if total_stray_person_ids > 0:
        print("\nWriting stray person_ids report to 'stray_person_ids_report.txt'...")
        report_filename = "stray_person_ids_report.txt"
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
