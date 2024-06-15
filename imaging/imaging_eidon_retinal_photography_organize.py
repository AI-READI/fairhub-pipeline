import os
import shutil
import imaging.imaging_classifying_rules as imaging_classifying_rules


def filter_eidon_files(file, outputfolder):
    """
    Filter and process EIDON files based on classification rules.

    This function applies classification rules to a DICOM file, extracts relevant information,
    and copies the file to an appropriate output directory based on the classification rule.

    Args:
        file (str): The path to the DICOM file to be processed.
        outputfolder (str): The directory where the processed files will be stored.

    Returns:
        dict: A dictionary containing information about the processed file, including rule, patient ID,
        patient name, laterality, rows, columns, SOP instance UID, series instance UID, filename,
        original file path, and any errors encountered.
    """

    filename = file.split("/")[-1]
    rule = imaging_classifying_rules.find_rule(file)
    b = imaging_classifying_rules.extract_dicom_entry(file)
    laterality = b.laterality
    uid = b.sopinstanceuid
    patientid = b.patientid
    rows = b.rows
    columns = b.columns
    seriesuid = b.seriesuid
    error = b.error
    name = b.name

    original_path = file
    output_path = f"{outputfolder}/{rule}/{rule}_{filename}"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.copyfile(original_path, output_path)

    dic = {
        "Rule": rule,
        "PatientID": patientid,
        "PatientName": name,
        "Laterality": laterality,
        "Rows": rows,
        "Columns": columns,
        "SOPInstanceuid": uid,
        "SeriesInstanceuid": seriesuid,
        "Filename": filename,
        "Path": file,
        "Error": error,
    }
    print(dic)
    return dic
