import matplotlib.pyplot as plt
import numpy as np
from pandas import Timestamp


def df_max(column):
    col_list = column.tolist()
    col_list = [
        (
            int(401)
            if x == "High"
            else (
                int(69)
                if x == "Low"
                else (
                    int(x)
                    if type(x) == float
                    else (
                        x
                        if type(x) == int
                        else x if type(x) == Timestamp else x if "T" in x else int(x)
                    )
                )
            )
        )
        for x in col_list
    ]
    return str(max(col_list))


def df_min(column):
    col_list = column.tolist()
    col_list = [
        (
            int(401)
            if x == "High"
            else (
                int(69)
                if x == "Low"
                else (
                    int(x)
                    if type(x) == float
                    else (
                        x
                        if type(x) == int
                        else x if type(x) == Timestamp else x if "T" in x else int(x)
                    )
                )
            )
        )
        for x in col_list
    ]
    return str(min(col_list))


def df_hist(column, filename, save_path=None):
    col_list = column.tolist()
    col_list = [
        (
            int(401)
            if x == "High"
            else (
                int(69)
                if x == "Low"
                else (
                    int(x)
                    if type(x) == float
                    else (
                        x
                        if type(x) == int
                        else x if type(x) == Timestamp else x if "T" in x else int(x)
                    )
                )
            )
        )
        for x in col_list
    ]
    arr = np.array(col_list, dtype="float32")
    counts, bins = np.histogram(arr)
    plt.figure(" ")
    plt.hist(arr, bins=bins)
    plt.xlabel("Value Ranges")  # Add x-axis label
    plt.ylabel("Frequency")  # Add y-axis label
    plt.title("Distribution of " + column.name + " in the input file")
    # plt.show()
    save_path = filename + "_plot.png"
    plt.savefig(save_path, bbox_inches="tight", dpi=300)
    plt.close()


def list_max(values):
    values = [
        (
            int(401)
            if x == "High"
            else (
                int(69)
                if x == "Low"
                else (
                    int(x)
                    if type(x) == float
                    else (
                        x
                        if type(x) == int
                        else x if type(x) == Timestamp else x if "T" in x else int(x)
                    )
                )
            )
        )
        for x in values
    ]
    return str(max(values))


def list_min(values):
    values = [
        (
            int(401)
            if x == "High"
            else (
                int(69)
                if x == "Low"
                else (
                    int(x)
                    if type(x) == float
                    else (
                        x
                        if type(x) == int
                        else x if type(x) == Timestamp else x if "T" in x else int(x)
                    )
                )
            )
        )
        for x in values
    ]
    return str(min(values))


def list_hist(values, title, filename):
    values = [
        (
            int(401)
            if x == "High"
            else (
                int(69)
                if x == "Low"
                else (
                    int(x)
                    if type(x) == float
                    else (
                        x
                        if type(x) == int
                        else x if type(x) == Timestamp else x if "T" in x else int(x)
                    )
                )
            )
        )
        for x in values
    ]
    arr = np.array(values, dtype="float32")
    counts, bins = np.histogram(arr)
    plt.figure(" ")
    plt.hist(arr, bins=bins)
    plt.xlabel("Value Ranges")  # Add x-axis label
    plt.ylabel("Frequency")  # Add y-axis label
    plt.title("Distribution of " + title + " in the output JSON file")  # Add plot title
    save_path = filename + "_output_" + title.replace(" ", "_") + "_plot.png"
    plt.savefig(save_path, bbox_inches="tight", dpi=300)
    plt.close()
