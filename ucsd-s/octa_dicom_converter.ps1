<#
.SYNOPSIS
    Spectralis DICOM Batch Converter

.DESCRIPTION
    Converts Spectralis data to DICOM format using the bundled SP-X_DICOM_Converter.exe.

    InputFolder is the parent folder containing batch folders to convert.
    Each batch folder inside is processed independently by the converter.

    OutputFolder is the parent folder where converted versions of the batch folders will
    be written. One subfolder is created here per input batch folder.

.PARAMETER InputFolder
    Parent folder containing batch folders to convert.

.PARAMETER OutputFolder
    Parent folder where converted versions of the batch folders will be written.
    Created if it does not already exist.

.EXAMPLE
    .\octa_dicom_converter.ps1 -InputFolder "D:\raw\spectralis-s" -OutputFolder "D:\converted\spectralis-s"

    Given an input like:
        D:\raw\spectralis-s\
            batch_001\
            batch_002\

    Output will be written as:
        D:\converted\spectralis-s\
            batch_001\
                converted\     <- DICOM files produced by the converter
            batch_002\
                converted\
            batch_001_error_log.txt  <- only written if that batch fails

.NOTES
    The converter executable is expected at:
        spx-dicom-converter\SP-X_DICOM_Converter.exe
    relative to this script's location. Do not move this script out of its folder.
#>

param (
    [Parameter(Mandatory = $true)]
    [string]$InputFolder,

    [Parameter(Mandatory = $true)]
    [string]$OutputFolder
)

# Resolve the converter executable relative to this script's location.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$DicomExecutable = Join-Path $ScriptDir "spx-dicom-converter\SP-X_DICOM_Converter.exe"

# Validate the converter executable exists before doing any work.
if (-not (Test-Path $DicomExecutable -PathType Leaf)) {
    Write-Error "Converter executable not found at:`n  $DicomExecutable`nMake sure the spx-dicom-converter folder is next to this script."
    exit 1
}

# Resolve input/output to absolute paths.
$InputFolder  = Resolve-Path $InputFolder -ErrorAction Stop | Select-Object -ExpandProperty Path
$OutputFolder = [System.IO.Path]::GetFullPath($OutputFolder)

if (-not (Test-Path $InputFolder -PathType Container)) {
    Write-Error "Input folder does not exist:`n  $InputFolder"
    exit 1
}

New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null

$Subfolders = Get-ChildItem -Path $InputFolder -Directory | Sort-Object Name

if ($Subfolders.Count -eq 0) {
    Write-Host "No subfolders found in $InputFolder. Nothing to do."
    exit 0
}

$Total     = $Subfolders.Count
$Succeeded = 0
$Failed    = 0

Write-Host "Found $Total folder(s) to process."
Write-Host "  Input  : $InputFolder"
Write-Host "  Output : $OutputFolder"
Write-Host ""

$i = 0
foreach ($Folder in $Subfolders) {
    $i++
    Write-Host "[$i/$Total] Processing: $($Folder.Name)"

    $SourceDir     = $Folder.FullName
    $TempOutputDir = Join-Path $OutputFolder $Folder.Name
    $OutputDir     = Join-Path $TempOutputDir "converted"

    try {
        New-Item -ItemType Directory -Path $TempOutputDir -Force | Out-Null

        $Process = Start-Process -FilePath $DicomExecutable `
                                 -ArgumentList "`"$SourceDir`"", "`"$OutputDir`"" `
                                 -Wait -PassThru -NoNewWindow

        if ($Process.ExitCode -ne 0) {
            throw "Converter exited with non-zero return code: $($Process.ExitCode)"
        }

        $Succeeded++
        Write-Host "  Done."
    }
    catch {
        $Failed++
        Write-Host "  ERROR: $_"

        # Write error details to a log file next to the output folder.
        $ErrorLogFile = Join-Path $OutputFolder "$($Folder.Name)_error_log.txt"
        $_ | Out-File -FilePath $ErrorLogFile -Encoding utf8
        Write-Host "  Error details written to: $ErrorLogFile"

        # Clean up partial output so incomplete data isn't left behind.
        if (Test-Path $TempOutputDir) {
            Remove-Item -Path $TempOutputDir -Recurse -Force
        }
    }
}

Write-Host ""
Write-Host "Finished. $Succeeded succeeded, $Failed failed."
if ($Failed -gt 0) {
    Write-Host "Check $OutputFolder for *_error_log.txt files."
}
