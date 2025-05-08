# & "C:\Users\b2aiUsr\.scripts\zeiss\bin\java.exe" -cp ".;C:\Program Files\MATLAB\MATLAB Runtime\v91\toolbox\javabuilder\jar\javabuilder.jar;C:\Users\b2aiUsr\.scripts\zeiss\cirrusDCMvisualizationsDICOMWrapper20240719_141654\cirrusDCMvisualizationsDICOMWrapper20240719_141654\*" demoVis C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407 C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407_CONVERTED 0

import subprocess

java_path = r"C:\Users\b2aiUsr\.scripts\zeiss\bin\java.exe"
classpath = r".;C:\Program Files\MATLAB\MATLAB Runtime\v91\toolbox\javabuilder\jar\javabuilder.jar;C:\Users\b2aiUsr\.scripts\zeiss\cirrusDCMvisualizationsDICOMWrapper20240719_141654\cirrusDCMvisualizationsDICOMWrapper20240719_141654\*"
main_class = "demoVis"
input_dir = r"C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407"
output_dir = r"C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407_CONVERTED"
additional_arg = "0"

command = [
    java_path,
    "-cp",
    classpath,
    main_class,
    input_dir,
    output_dir,
    additional_arg,
]

returncode = subprocess.call(
    [java_path, "-cp", classpath, main_class, input_dir, output_dir, additional_arg]
)

if returncode == 0:
    print("Command executed successfully")
else:
    print(f"Command failed with return code {returncode}")

# try:
#     result = subprocess.run(command, check=True, capture_output=True, text=True)
#     print("Command executed successfully")
#     print("Output:", result.stdout)
# except subprocess.CalledProcessError as e:
#     print("An error occurred while executing the command")
#     print("Error output:", e.stderr)
