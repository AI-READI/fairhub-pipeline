"""Run the Spectralis DICOM converter against several DICOMDIR variants to compare output IDs.

Each batch folder in input_folder contains the regenerated ``DICOMDIR`` and the original
(broken) ``DICOMDIR_old``. The converter always reads a file named ``DICOMDIR``, so for each
variant we build a staging copy of the batch folder where every file is hardlinked (no extra
disk space) and the chosen DICOMDIR file is copied in as ``DICOMDIR``. The converter is then
run on each staging copy in turn, writing to a separate output folder per variant.
Staging folders are kept so each run's exact input can be inspected afterwards.
"""

import os
import shutil
import subprocess
from traceback import format_exc

# Spectralis DICOM converter executable path
dicom_executable_location = os.path.abspath(
    "C:\\Users\\sanjay\\Downloads\\spx-dicom-converter\\SP-X_DICOM_Converter.exe"
)

input_folder = os.path.abspath("D:\\year3+raw\\spectralis-s")
# Staging inputs (hardlinks) and outputs; keep staging on the same drive as input_folder
staging_root = os.path.abspath("D:\\year3+stage\\spectralis-s")
output_root = os.path.abspath("D:\\year3+pre\\spectralis-s-dicomdir-compare")

# Files in each batch folder that are DICOMDIR candidates (never linked into staging)
DICOMDIR_FILES = {"DICOMDIR", "DICOMDIR_old"}

# variant name -> which file in the batch folder to use as DICOMDIR
VARIANTS = {
    "old_run1": "DICOMDIR_old",
    "old_run2": "DICOMDIR_old",
    "new": "DICOMDIR",
}


def link_or_copy(src, dst):
    """Hardlink src to dst, falling back to a copy if hardlinking is not possible."""
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def build_staging_folder(source_dir, staging_dir, dicomdir_name):
    """Mirror source_dir into staging_dir via hardlinks, using dicomdir_name as DICOMDIR."""
    if os.path.isdir(staging_dir):
        shutil.rmtree(staging_dir)

    for root, _dirs, files in os.walk(source_dir):
        rel = os.path.relpath(root, source_dir)
        target_root = os.path.join(staging_dir, rel)
        os.makedirs(target_root, exist_ok=True)
        for file_name in files:
            if rel == "." and file_name in DICOMDIR_FILES:
                continue
            link_or_copy(os.path.join(root, file_name), os.path.join(target_root, file_name))

    # Copy (not link) so the converter can never modify the source DICOMDIR files
    shutil.copy2(
        os.path.join(source_dir, dicomdir_name), os.path.join(staging_dir, "DICOMDIR")
    )


def run_variant(variant, dicomdir_name, batch_folders):
    """Stage and convert every batch folder for one variant."""
    variant_output = os.path.join(output_root, variant)
    os.makedirs(variant_output, exist_ok=True)

    for folder_name in batch_folders:
        source_dir = os.path.join(input_folder, folder_name)
        staging_dir = os.path.join(staging_root, variant, folder_name)
        output_dir = os.path.join(variant_output, folder_name, "converted")
        log_file = os.path.join(variant_output, f"{folder_name}_converter_log.txt")

        if not os.path.isfile(os.path.join(source_dir, dicomdir_name)):
            print(f"[{variant}] Skipping {folder_name}: no {dicomdir_name}")
            continue

        try:
            print(f"[{variant}] Staging {folder_name} with {dicomdir_name}")
            build_staging_folder(source_dir, staging_dir, dicomdir_name)

            os.makedirs(os.path.dirname(output_dir), exist_ok=True)
            print(f"[{variant}] Running executable for {folder_name}")
            with open(log_file, "w") as log:
                return_code = subprocess.call(
                    [dicom_executable_location, staging_dir, output_dir],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            print(f"[{variant}] {folder_name} finished with exit code {return_code}")
        except Exception as e:
            print(f"[{variant}] {folder_name} failed with error: {e}")
            error_log_file = os.path.join(variant_output, f"{folder_name}_error_log.txt")
            with open(error_log_file, "w") as f:
                f.write(format_exc())


def main():
    if not os.path.isdir(input_folder):
        raise SystemExit(f"Input folder does not exist: {input_folder}")

    batch_folders = [
        name
        for name in os.listdir(input_folder)
        if os.path.isdir(os.path.join(input_folder, name))
    ]
    if not batch_folders:
        print(f"No subfolders found in {input_folder}")
        return

    for variant, dicomdir_name in VARIANTS.items():
        run_variant(variant, dicomdir_name, batch_folders)

    print(f"Done. Outputs are in {output_root}, staging inputs are in {staging_root}")


if __name__ == "__main__":
    main()
