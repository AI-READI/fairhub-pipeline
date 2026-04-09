@echo off
::
:: Spectralis DICOM Batch Converter
:: ----------------------------------
:: Converts Spectralis data to DICOM format using the bundled SP-X_DICOM_Converter.exe.
::
:: input_folder is the parent folder containing batch folders to convert.
:: Each batch folder inside is processed independently by the converter.
::
:: output_folder is the parent folder where converted versions of the batch folders will
:: be written. One subfolder is created here per input batch folder.
::
:: Usage:
::   octa_dicom_converter.cmd <input_folder> <output_folder>
::
:: Arguments:
::   input_folder   Parent folder containing batch folders to convert.
::   output_folder  Parent folder where converted versions of the batch folders will be
::                  written (created if it does not already exist).
::
:: Example:
::   octa_dicom_converter.cmd D:\raw\spectralis-s D:\converted\spectralis-s
::
::   Given an input like:
::     D:\raw\spectralis-s\
::         batch_001\
::         batch_002\
::
::   Output will be written as:
::     D:\converted\spectralis-s\
::         batch_001\
::             converted\     <- DICOM files produced by the converter
::         batch_002\
::             converted\
::         batch_001_error_log.txt  <- only written if that batch fails
::
:: The converter executable is expected at:
::   spx-dicom-converter\SP-X_DICOM_Converter.exe
:: relative to this script's location. Do not move this script out of its folder.
::

setlocal enabledelayedexpansion

:: Validate arguments
if "%~1"=="" (
    echo ERROR: Missing input_folder argument.
    echo Usage: octa_dicom_converter.cmd ^<input_folder^> ^<output_folder^>
    exit /b 1
)
if "%~2"=="" (
    echo ERROR: Missing output_folder argument.
    echo Usage: octa_dicom_converter.cmd ^<input_folder^> ^<output_folder^>
    exit /b 1
)

:: Resolve paths relative to this script's location
set "SCRIPT_DIR=%~dp0"
set "DICOM_EXE=%SCRIPT_DIR%spx-dicom-converter\SP-X_DICOM_Converter.exe"
set "INPUT_FOLDER=%~f1"
set "OUTPUT_FOLDER=%~f2"

:: Validate converter executable
if not exist "%DICOM_EXE%" (
    echo ERROR: Converter executable not found at:
    echo   %DICOM_EXE%
    echo Make sure the spx-dicom-converter folder is next to this script.
    exit /b 1
)

:: Validate input folder
if not exist "%INPUT_FOLDER%\" (
    echo ERROR: Input folder does not exist:
    echo   %INPUT_FOLDER%
    exit /b 1
)

:: Create output folder if it doesn't exist
if not exist "%OUTPUT_FOLDER%\" mkdir "%OUTPUT_FOLDER%"

:: Count subfolders for progress display
set /a TOTAL=0
for /d %%D in ("%INPUT_FOLDER%\*") do set /a TOTAL+=1

if %TOTAL%==0 (
    echo No subfolders found in %INPUT_FOLDER%. Nothing to do.
    exit /b 0
)

echo Found %TOTAL% folder(s) to process.
echo   Input  : %INPUT_FOLDER%
echo   Output : %OUTPUT_FOLDER%
echo.

set /a CURRENT=0
set /a SUCCEEDED=0
set /a FAILED=0

for /d %%D in ("%INPUT_FOLDER%\*") do (
    set /a CURRENT+=1
    set "FOLDER_NAME=%%~nxD"
    set "SOURCE_DIR=%%D"
    set "TEMP_OUTPUT_DIR=%OUTPUT_FOLDER%\%%~nxD"
    set "OUTPUT_DIR=%OUTPUT_FOLDER%\%%~nxD\converted"

    echo [!CURRENT!/%TOTAL%] Processing: !FOLDER_NAME!

    if not exist "!TEMP_OUTPUT_DIR!\" mkdir "!TEMP_OUTPUT_DIR!"

    "%DICOM_EXE%" "!SOURCE_DIR!" "!OUTPUT_DIR!"
    set "EXIT_CODE=!errorlevel!"

    if !EXIT_CODE! neq 0 (
        set /a FAILED+=1
        echo   ERROR: Converter exited with code !EXIT_CODE!
        echo Converter exited with code !EXIT_CODE! > "%OUTPUT_FOLDER%\!FOLDER_NAME!_error_log.txt"
        echo   Error details written to: %OUTPUT_FOLDER%\!FOLDER_NAME!_error_log.txt
        if exist "!TEMP_OUTPUT_DIR!\" rmdir /s /q "!TEMP_OUTPUT_DIR!"
    ) else (
        set /a SUCCEEDED+=1
        echo   Done.
    )
)

echo.
echo Finished. %SUCCEEDED% succeeded, %FAILED% failed.
if %FAILED% gtr 0 (
    echo Check %OUTPUT_FOLDER% for *_error_log.txt files.
)

endlocal
exit /b %FAILED%
