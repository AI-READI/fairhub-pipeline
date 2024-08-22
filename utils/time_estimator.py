import os
import tempfile
import time


class TimeEstimator:
    """Class for handling calculating estimated time of arrival"""

    def __init__(self):
        self.start_time = time.time()
        # self.end_time = end_time
        self.total_processed_files: int = 0
        self.processed_seconds: float = 0.0

    def eta(self, file_paths):
        self.total_processed_files += 1

        end_time = time.time()
        remaining_files = len(file_paths) - self.total_processed_files

        self.processed_seconds += (end_time - self.start_time)

        average_time = self.processed_seconds / self.total_processed_files
        eta = average_time * remaining_files
        print("eta is", eta, "secs")
        return eta
