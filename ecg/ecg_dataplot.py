import logging


import wfdb
import matplotlib.pyplot as plt


dataplot_logger = logging.getLogger("ecg.dataplot")


def make_dataplot(conv_dict, output_folder):
    """Create a plot for visual inspection of ECG signals.
    Args:
        conv_dict (dict): must contain at least 3 valid elements:
            participantID
            output_hea_file
            output_dat_file
        output_folder (string): full path to a folder for the saved plot
    Returns:
        fig_path (string): full path to the saved plot
    """
    dataplot_dict = dict()
    w_full = conv_dict["output_hea_file"].replace(".hea", "")
    dataplot_logger.info(f"Creating plot for {w_full}")

    record = wfdb.rdrecord(w_full)

    w_base = w_full.split("/")[-1]
    fig_handle_grids = wfdb.plot_wfdb(
        record, figsize=(10, 14), ecg_grids="all", return_fig=True
    )
    full_plot_path = f"{output_folder}/{w_base}__wfdb_fig_ecg_grids.png"
    _ = fig_handle_grids.savefig(full_plot_path)

    plt.close("all")

    dataplot_dict["participantID"] = conv_dict["participantID"]
    dataplot_dict["output_files"] = [full_plot_path]

    return dataplot_dict
