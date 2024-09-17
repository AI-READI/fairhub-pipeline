import datetime
import time
from collections import OrderedDict

INTERVALS = OrderedDict(
    [
        ("millennium", 31536000000),  # 60 * 60 * 24 * 365 * 1000
        ("century", 3153600000),  # 60 * 60 * 24 * 365 * 100
        ("year", 31536000),  # 60 * 60 * 24 * 365
        ("month", 2627424),  # 60 * 60 * 24 * 30.41 (assuming 30.41 days in a month)
        ("week", 604800),  # 60 * 60 * 24 * 7
        ("day", 86400),  # 60 * 60 * 24
        ("hour", 3600),  # 60 * 60
        ("minute", 60),
        ("second", 1),
    ]
)


def human_time(seconds, decimals=1):
    """Human-readable time from seconds (ie. 5 days and 2 hours).
    Examples:
        >>> human_time(15)
        '15 seconds'
        >>> human_time(3600)
        '1 hour'
        >>> human_time(3720)
        '1 hour and 2 minutes'
        >>> human_time(266400)
        '3 days and 2 hours'
        >>> human_time(-1.5)
        '-1.5 seconds'
        >>> human_time(0)
        '0 seconds'
        >>> human_time(0.1)
        '100 milliseconds'
        >>> human_time(1)
        '1 second'
        >>> human_time(1.234, 2)
        '1.23 seconds'
    Args:
        seconds (int or float): Duration in seconds.
        decimals (int): Number of decimals.
    Returns:
        str: Human-readable time.
    """
    input_is_int = isinstance(seconds, int)
    if seconds < 0:
        return str(seconds if input_is_int else round(seconds, decimals)) + " seconds"
    elif seconds == 0:
        return "0 seconds"
    elif 0 < seconds < 1:
        # Return in milliseconds.
        ms = int(seconds * 1000)
        return "%i millisecond%s" % (ms, "s" if ms != 1 else "")
    elif 1 < seconds < INTERVALS["minute"]:
        return str(seconds if input_is_int else round(seconds, decimals)) + " seconds"

    res = []
    for interval, count in INTERVALS.items():
        quotient, remainder = divmod(seconds, count)
        if quotient >= 1:
            seconds = remainder
            if quotient > 1:
                # Plurals.
                if interval == "millennium":
                    interval = "millennia"
                elif interval == "century":
                    interval = "centuries"
                else:
                    interval += "s"
            res.append("%i %s" % (int(quotient), interval))
        if remainder == 0:
            break

    if len(res) >= 2:
        # Only shows 2 most important intervals.
        return "{} and {}".format(res[0], res[1])
    return res[0]


class TimeEstimator:
    """Class for handling calculating estimated time of arrival"""

    def __init__(self, total_number_of_files: int):
        self.start_time = time.time()

        # variables for the calculation of the ETA
        self.total_number_of_files = total_number_of_files
        self.total_processed_files: int = 0

    @property
    def elapsed_time(self):
        end_time = time.time()
        return end_time - self.start_time

    @property
    def eta(self):
        average_time = self.elapsed_time / self.total_processed_files
        return average_time * (self.total_number_of_files - self.total_processed_files)

    @property
    def finish_time(self):
        """calculate finish time"""

        current_time = datetime.datetime.now()
        finish_time = current_time + datetime.timedelta(seconds=self.eta)
        return finish_time.strftime("%H:%M:%S")

    def currenttz(self):
        if time.daylight:
            return datetime.timezone(
                datetime.timedelta(seconds=-time.altzone), time.tzname[1]
            )
        else:
            return datetime.timezone(
                datetime.timedelta(seconds=-time.timezone), time.tzname[0]
            )

    def step(self):
        self.total_processed_files += 1

        return f"{self.total_processed_files}/{self.total_number_of_files} [{self.total_processed_files / self.total_number_of_files * 100:.2f}%] in {human_time(self.elapsed_time)} ~ {human_time(self.elapsed_time / self.total_processed_files)} per item | ETA: in about {human_time(self.eta)} or at {self.finish_time} {self.currenttz()}"
