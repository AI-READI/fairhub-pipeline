import os
import tempfile
import datetime
import time


class TimeEstimator:
    """Class for handling calculating estimated time of arrival"""

    def __init__(self, file_paths):
        self.start_time = time.time()

        # variables for the calculation of the ETA
        self.total_processed_files: int = 0
        self.elapsed_time: float = 0.0
        self.file_paths = file_paths

    def get_eta(self):
        end_time = time.time()

        self.elapsed_time = (end_time - self.start_time)
        average_time = self.elapsed_time / self.total_processed_files
        return average_time * (len(self.file_paths) - self.total_processed_files)

    def progress(self) -> int:
        self.total_processed_files += 1
        eta: float = self.get_eta()
        finish_time = datetime.datetime.fromtimestamp(eta).strftime("%H:%M:%S")

        print("***", "time elapsed:", self.elapsed_time, "time left", eta,
              "estimated finish time:", finish_time)
        return eta
