import requests
import contextlib
import config
import threading
from colorama import just_fix_windows_console, Fore, Back, Style

just_fix_windows_console()


class Logwatch:
    """Class for sending logging messages to the logwatch server'"""

    def __init__(self, channel: str = "drain", print: bool = False):
        self.print = print

        self.drain = config.FAIRHUB_CATCH_ALL_LOG_ENDPOINT
        self.triton_drain = config.FAIRHUB_TRITON_LOG_ENDPOINT
        self.maestro2_drain = config.FAIRHUB_MAESTRO2_LOG_ENDPOINT
        self.eidon_drain = config.FAIRHUB_EIDON_LOG_ENDPOINT
        self.ecg_drain = config.FAIRHUB_ECG_LOG_ENDPOINT
        self.cgm_drain = config.FAIRHUB_CGM_LOG_ENDPOINT
        self.env_sensor_drain = config.FAIRHUB_ENV_SENSOR_LOG_ENDPOINT
        self.fitness_tracker_drain = config.FAIRHUB_FITNESS_TRACKER_LOG_ENDPOINT
        self.optomed_drain = config.FAIRHUB_OPTOMED_LOG_ENDPOINT
        self.flio_drain = config.FAIRHUB_FLIO_LOG_ENDPOINT
        self.spectralis_drain = config.FAIRHUB_SPECTRALIS_LOG_ENDPOINT
        self.cirrus_drain = config.FAIRHUB_CIRRUS_LOG_ENDPOINT

        if (channel == "triton") and (self.triton_drain is not None):
            self.drain = self.triton_drain
        if (channel == "maestro2") and (self.maestro2_drain is not None):
            self.drain = self.maestro2_drain
        if (channel == "eidon") and (self.eidon_drain is not None):
            self.drain = self.eidon_drain
        if (channel == "ecg") and (self.ecg_drain is not None):
            self.drain = self.ecg_drain
        if (channel == "cgm") and (self.cgm_drain is not None):
            self.drain = self.cgm_drain
        if (channel == "env_sensor") and (self.env_sensor_drain is not None):
            self.drain = self.env_sensor_drain
        if (channel == "fitness_tracker") and (self.fitness_tracker_drain is not None):
            self.drain = self.fitness_tracker_drain
        if (channel == "optomed") and (self.optomed_drain is not None):
            self.drain = self.optomed_drain
        if (channel == "flio") and (self.flio_drain is not None):
            self.drain = self.flio_drain
        if (channel == "spectralis") and (self.spectralis_drain is not None):
            self.drain = self.spectralis_drain
        if (channel == "cirrus") and (self.cirrus_drain is not None):
            self.drain = self.cirrus_drain

    def trace(self, message: str):
        """Send a trace message to the logwatch server"""
        if self.print:
            print(Style.DIM + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "trace", "message": message}),
            ).start()

    def noPrintTrace(self, message: str):
        """Send a trace message to the logwatch server without printing"""
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "trace", "message": message}),
            ).start()

    def debug(self, message: str):
        """Send a debug message to the logwatch server"""
        if self.print:
            print(Fore.BLUE + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "debug", "message": message}),
            ).start()

    def info(self, message: str):
        """Send an info message to the logwatch server"""
        if self.print:
            print(Fore.CYAN + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "info", "message": message}),
            ).start()

    def fastInfo(self, message: str):
        """Send a threaded info message to the logwatch server. Used for items that need to be processed quickly"""
        if self.print:
            print(Fore.CYAN + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "info", "message": message}),
            )

    def error(self, message: str):
        """Send an error message to the logwatch server"""
        if self.print:
            print(Fore.RED + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            requests.post(self.drain, json={"level": "error", "message": message})

    def warn(self, message: str):
        """Send a warning message to the logwatch server"""
        if self.print:
            print(Fore.YELLOW + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            requests.post(self.drain, json={"level": "warning", "message": message})

    def critical(self, message: str):
        """Send a critical message to the logwatch server"""
        if self.print:
            print(Back.RED + Fore.WHITE + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            requests.post(self.drain, json={"level": "critical", "message": message})

    def time(self, message: str):
        """Send a time message to the logwatch server"""
        if self.print:
            print(Back.GREEN + Fore.WHITE + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            # Not threaded because it's a time message
            requests.post(self.drain, json={"level": "time", "message": message})

    def fastTime(self, message: str):
        """Send a threaded time message to the logwatch server. Used for items that need to be processed quickly"""
        if self.print:
            print(Back.GREEN + Fore.WHITE + message + Style.RESET_ALL)
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "time", "message": message}),
            ).start()

    def noPrintTime(self, message: str):
        """Send a threaded time message to the logwatch server. Used for items that need to be processed quickly"""
        with contextlib.suppress(Exception):
            threading.Thread(
                target=requests.post,
                args=(self.drain, {"level": "time", "message": message}),
            ).start()
