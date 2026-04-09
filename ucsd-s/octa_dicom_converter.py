"""
Spectralis DICOM Batch Converter
---------------------------------
Converts Spectralis data to DICOM format using the bundled SP-X_DICOM_Converter.exe.

Usage:
    python octa_dicom_converter.py <input_folder> <output_folder>

Arguments:
    input_folder   Parent folder containing batch folders to convert.
                   Each batch folder is processed independently by the converter.
    output_folder  Parent folder where converted versions of the batch folders will be
                   written. One subfolder is created here per input batch folder.
                   Will be created if it does not already exist.

Example:
    python octa_dicom_converter.py D:\\raw\\spectralis-s D:\\converted\\spectralis-s

    Given an input like:
        D:\\raw\\spectralis-s\\
            batch_001\\
            batch_002\\

    Output will be written as:
        D:\\converted\\spectralis-s\\
            batch_001\\
                converted\\     <- DICOM files produced by the converter
            batch_002\\
                converted\\
            batch_001_error_log.txt  <- only written if that batch fails

The converter executable is expected at:
    spx-dicom-converter/SP-X_DICOM_Converter.exe
(relative to this script's location — do not move this script out of its folder)
"""

import argparse
import os
import shutil
import subprocess
from traceback import format_exc

# Resolve the converter executable relative to this script so it works
# regardless of the current working directory when the script is invoked.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DICOM_EXECUTABLE = os.path.join(
    SCRIPT_DIR, "spx-dicom-converter", "SP-X_DICOM_Converter.exe"
)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Batch-convert Spectralis subject folders to DICOM using "
            "SP-X_DICOM_Converter.exe."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input_folder",
        help="Parent folder containing batch folders to convert.",
    )
    parser.add_argument(
        "output_folder",
        help=(
            "Parent folder where converted versions of the batch folders will be written. "
            "Created if it does not already exist."
        ),
    )
    args = parser.parse_args()

    input_folder = os.path.abspath(args.input_folder)
    output_folder = os.path.abspath(args.output_folder)

    # Validate the converter executable exists before doing any work.
    if not os.path.isfile(DICOM_EXECUTABLE):
        raise SystemExit(
            f"ERROR: Converter executable not found at:\n  {DICOM_EXECUTABLE}\n"
            "Make sure the spx-dicom-converter folder is next to this script."
        )

    if not os.path.isdir(input_folder):
        raise SystemExit(
            f"ERROR: Input folder does not exist:\n  {input_folder}"
        )

    os.makedirs(output_folder, exist_ok=True)

    subfolders = sorted(
        name
        for name in os.listdir(input_folder)
        if os.path.isdir(os.path.join(input_folder, name))
    )

    if not subfolders:
        print(f"No subfolders found in {input_folder}. Nothing to do.")
        return

    total = len(subfolders)
    print(f"Found {total} folder(s) to process.")
    print(f"  Input  : {input_folder}")
    print(f"  Output : {output_folder}")
    print()

    succeeded = 0
    failed = 0

    for i, folder_name in enumerate(subfolders, start=1):
        print(f"[{i}/{total}] Processing: {folder_name}")

        source_dir = os.path.join(input_folder, folder_name)
        temp_output_dir = None

        try:
            temp_output_dir = os.path.join(output_folder, folder_name)
            os.makedirs(temp_output_dir, exist_ok=True)
            output_dir = os.path.join(temp_output_dir, "converted")

            result = subprocess.call([DICOM_EXECUTABLE, source_dir, output_dir])

            if result != 0:
                raise RuntimeError(
                    f"Converter exited with non-zero return code: {result}"
                )

        except Exception as e:
            failed += 1
            error_log = format_exc()
            print(f"  ERROR: {e}")

            # Write the full traceback to a log file next to the output folder.
            error_log_file = os.path.join(output_folder, f"{folder_name}_error_log.txt")
            with open(error_log_file, "w") as f:
                f.write(error_log)
            print(f"  Error details written to: {error_log_file}")

            # Clean up the partial output folder so incomplete data isn't left behind.
            if temp_output_dir and os.path.isdir(temp_output_dir):
                shutil.rmtree(temp_output_dir, ignore_errors=True)
            continue

        succeeded += 1
        print("  Done.")

    print()
    print(f"Finished. {succeeded} succeeded, {failed} failed.")
    if failed:
        print(f"Check {output_folder} for *_error_log.txt files.")


if __name__ == "__main__":
    main()
