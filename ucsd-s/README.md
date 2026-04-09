# Spectralis DICOM Batch Converter

Batch-converts Spectralis subject/session folders to DICOM format using the bundled `SP-X_DICOM_Converter.exe`.

## Folder structure

```text
ucsd-s/
├── spx-dicom-converter/        # bundled converter — do not move
│   └── SP-X_DICOM_Converter.exe
├── octa_dicom_converter.py
├── octa_dicom_converter.ps1
├── octa_dicom_converter.cmd
└── README.md
```

## Usage

### Option A — Python (requires Python 3.6+)

```bat
python octa_dicom_converter.py <input_folder> <output_folder>
```

### Option B — PowerShell (no Python needed)

```powershell
.\octa_dicom_converter.ps1 -InputFolder <input_folder> -OutputFolder <output_folder>
```

> If PowerShell blocks the script with an execution policy error, run this once in an elevated PowerShell window:
>
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```

### Option C — Command Prompt / batch file (no Python or PowerShell needed)

```bat
octa_dicom_converter.cmd <input_folder> <output_folder>
```

## Arguments

| Argument        | Description                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------------------ |
| `input_folder`  | Parent folder containing batch folders to convert. Each batch folder inside is processed independently.            |
| `output_folder` | Parent folder where converted versions of the batch folders will be written. Created if it does not already exist. |

## Example

Given an input like:

```text
D:\raw\spectralis-s\
    batch_001\
    batch_002\
```

Run:

```bat
octa_dicom_converter.cmd D:\raw\spectralis-s D:\converted\spectralis-s
```

Output will be written as:

```text
D:\converted\spectralis-s\
    batch_001\
        converted\     <- DICOM files produced by the converter
    batch_002\
        converted\
    batch_001_error_log.txt  <- only written if that batch fails
```
