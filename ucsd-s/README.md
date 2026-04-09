# Spectralis DICOM Batch Converter

Batch-converts Spectralis subject/session folders to DICOM format using the bundled `SP-X_DICOM_Converter.exe`.

## Folder structure

```
ucsd-s/
├── spx-dicom-converter/        # bundled converter — do not move
│   └── SP-X_DICOM_Converter.exe
├── octa_dicom_converter.py
├── octa_dicom_converter.ps1
└── README.md
```

## Usage

### Option A — Python (requires Python 3.6+)

```
python octa_dicom_converter.py <input_folder> <output_folder>
```

### Option B — PowerShell (no Python needed)

```
.\octa_dicom_converter.ps1 -InputFolder <input_folder> -OutputFolder <output_folder>
```

> If PowerShell blocks the script with an execution policy error, run this once in an elevated PowerShell window:
> ```
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```

## Arguments

| Argument | Description |
|---|---|
| `input_folder` | Folder containing one subfolder per subject/session |
| `output_folder` | Destination folder for converted output (created if missing) |

## Output structure

```
<output_folder>/
    <subject_folder>/
        converted/      ← DICOM files produced by the converter
    <subject_folder>_error_log.txt   ← only written if that folder fails
```

If a folder fails to convert, it is cleaned up and an error log is written. Processing continues for the remaining folders.
