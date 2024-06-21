import json
import time
import os


class WorkflowFileDependencies:
    """Class for storing information about which input files generate which output files in a workflow
    The structure will in the format:
    [
        {
            "input_files": [file1, file2],
            "output_files": [file3, file4]
        },
        {
            "input_files": [file5],
            "output_files": [file6, file7]
        }
    ]
    """

    def __init__(self, dependencies=None):
        self.dependencies = [] if dependencies is None else dependencies

    def add_dependency(self, input_files: list[str], output_files: list[str]):
        """Add a dependency between input_files and output_files"""
        self.dependencies.append(
            {"input_files": input_files, "output_files": output_files}
        )

    def write_to_file(self, folder_path: str):
        """Write the dependencies to a file"""
        timestr = time.strftime("%Y%m%d-%H%M%S")
        file_name = f"file_map_{timestr}.json"

        file_path = os.path.join(folder_path, file_name)

        with open(file_path, "w") as f:
            json.dump(self.dependencies, f, indent=4)

        return {"file_path": file_path, "file_name": file_name}
