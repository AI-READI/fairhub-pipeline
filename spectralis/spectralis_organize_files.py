import pydicom
import os
import shutil

SOP_CLASS_UID_MAP = {
    "1.2.840.10008.5.1.4.1.1.77.1.5.7": ["enface"],
    "1.2.840.10008.5.1.4.1.1.77.1.5.8": ["vol"],
    "1.2.840.10008.5.1.4.1.1.66": ["raw_storage"],
    "1.2.840.10008.5.1.4.1.1.66.8": ["heightmap"],
    "1.2.840.10008.5.1.4.1.1.77.1.5.4": ["opt"],
    "1.2.840.10008.5.1.4.1.1.77.1.5.1": ["op"],
}


def get_words_for_uid(uid):
    """
    Given a UID, returns the associated words from SOP_CLASS_UID_MAP.
    :param uid: The UID to search for (string).
    :return: List of associated words or "t UID nofound".
    """
    return SOP_CLASS_UID_MAP.get(uid, ["UID not found"])


def process_octa(
    base_dir, output_dir, sop_class_uid="1.2.840.10008.5.1.4.1.1.77.1.5.8"
):
    """
    Organizes DICOM files based on specific SOPClassUID, StudyTime, and SOPInstanceUID.

    :param base_dir: The base directory to search for DICOM files
    :param output_dir: The directory to create subfolders for organized files
    :param sop_class_uid: The SOPClassUID to filter initial files (default is Secondary Capture Image Storage UID)
    """
    # Walk through the base directory to find all DICOM files
    for root, _, files in os.walk(base_dir):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Read DICOM file metadata
                ds = pydicom.dcmread(file_path)

                # Check if file matches the target SOPClassUID
                if hasattr(ds, "SOPClassUID") and ds.SOPClassUID == sop_class_uid:
                    # Create a subfolder based on the file's SOPInstanceUID
                    subfolder_name = f"{ds.PatientID}_{ds.Laterality}_spectralis_mac_20x20_hs_octa_oct_{ds.SOPInstanceUID}"
                    subfolder_path = os.path.join(output_dir, subfolder_name)
                    os.makedirs(subfolder_path, exist_ok=True)

                    # Generate new filename with words mapped from SOP_CLASS_UID_MAP
                    sop_words = "_".join(get_words_for_uid(ds.SOPClassUID))
                    new_filename = f"{os.path.splitext(file)[0]}_{sop_words}{os.path.splitext(file)[1]}"
                    new_file_path = os.path.join(subfolder_path, new_filename)

                    # Copy the current file to the created folder with the new name
                    shutil.copy(file_path, new_file_path)

                    # Get StudyTime and SOPInstanceUID of the file
                    study_time = getattr(ds, "StudyTime", None)
                    sop_instance_uid1 = (
                        ds.ReferencedSeriesSequence[0]
                        .ReferencedInstanceSequence[0]
                        .ReferencedSOPInstanceUID
                    )
                    sop_instance_uid2 = (
                        ds.ReferencedSeriesSequence[1]
                        .ReferencedInstanceSequence[0]
                        .ReferencedSOPInstanceUID
                    )

                    # Add files with the same StudyTime or SOPInstanceUID to the same folder
                    for root2, _, files2 in os.walk(base_dir):
                        for file2 in files2:
                            file2_path = os.path.join(root2, file2)
                            try:
                                ds2 = pydicom.dcmread(file2_path)

                                # Generate new filename with words mapped from SOP_CLASS_UID_MAP
                                sop_words2 = "_".join(
                                    get_words_for_uid(ds2.SOPClassUID)
                                )
                                new_file2_name = f"{os.path.splitext(file2)[0]}_{sop_words2}{os.path.splitext(file2)[1]}"
                                new_file2_path = os.path.join(
                                    subfolder_path, new_file2_name
                                )

                                # Copy the file if it matches StudyTime or SOPInstanceUID
                                if getattr(ds2, "StudyTime", None) == study_time:
                                    shutil.copy(file2_path, new_file2_path)
                                elif getattr(ds2, "SOPInstanceUID", None) in [
                                    sop_instance_uid1,
                                    sop_instance_uid2,
                                ]:
                                    shutil.copy(file2_path, new_file2_path)
                            except Exception as e:
                                print(f"Error reading file {file2_path}: {e}")

            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
