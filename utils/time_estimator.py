import math
import datetime
import time


class TimeEstimator:
    """Class for handling calculating estimated time of arrival"""

    def __init__(self, total_file):
        self.start_time = time.time()

        # variables for the calculation of the ETA
        self.total_processed_files: int = 0
        self.total_file = total_file

    @property
    def elapsed_time(self):
        end_time = time.time()
        elapsed_time = (end_time - self.start_time)
        return elapsed_time

    @property
    def eta(self):
        average_time = self.elapsed_time / self.total_processed_files
        return average_time * (self.total_file - self.total_processed_files)

    @property
    def finish_time(self):
        """ calculate finish time """

        current_time = datetime.datetime.now()
        finish_time = current_time + datetime.timedelta(seconds=self.eta)
        formatted_finish_time = finish_time.strftime("%H:%M:%S")

        return formatted_finish_time

    def step(self):
        self.total_processed_files += 1

        # converts days, hours, minutes, and seconds into readable format
        total_seconds = self.eta
        days = total_seconds // (60 * 60 * 24)
        total_seconds %= (60 * 60 * 24)
        hours = total_seconds // (60 * 60)
        total_seconds %= (60 * 60)
        minutes = total_seconds // 60
        total_seconds %= 60
        seconds = total_seconds
        return (
            f"Time elapsed: {round(self.elapsed_time)} seconds,"
            f"time left: {' '.join([f'{days} days,' if days else '', f'{hours} hours,' if hours else '', f'{int(minutes)} minutes' if minutes else ''])} "
            f"{round(seconds)} seconds, estimated finish time: {self.finish_time}"
        )

