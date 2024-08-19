import logging

import pandas as pd
from datetime import datetime, timedelta

import matplotlib.pyplot as plt  # need to make the plots
import matplotlib.axis as plt_axis  # to catch the class
import matplotlib.axes as plt_axes  # to catch the class
import matplotlib.dates as mdates  # to use ConciseDateFormatter

# Note that this set of plotting utilities
# - does not require the ES class
# - is tailored to work with the ES class data in a pandas dataframe
# - is not required for conversion of the EnvSensor data
# - may or may not be included in the production workflow to provide plots for quality checks

utils_plot_logger = logging.getLogger('es.utils_plot')


def read_skiprow_count_from_file(fname):
    skiprows = 0

    with open(fname, 'r') as f:
        try:
            myline = f.readline().strip()

            if myline[0] in [';', '#']:  # first lines should start with ;
                first_word = myline[1:].split(':')[0].strip()
                if first_word in ['header_lines']:
                    skiprows = int(myline.split(':')[-1].strip())
        except Exception as e:
            utils_plot_logger.error(f'Exception {e} when reading first line of {fname}')
            skiprows = -1

    return skiprows


def create_spectral_plot(df, plot_ht=3, ax=None, add_legend=True,
                         add_suptitle=False, yaxis_adjusts_to_data=False, verbose=False):
    ''' Creates a plot with one line for each band of the light spectrum
        Args:
            df (dataframe): ES data as a pandas dataframe
            plot_ht (integer): plot height
            ax (matplotlib axes): subplot axes where this plot will be displayed
            add_legend (boolean): whether to include a legend with line colors representing the spectral band
            add_suptitle (boolean): whether to add a title for this subplot
            yaxis_adjusts_to_data (boolean):
                True - the y-axis zooms to the amount of data. Handy for seeing
                    data with small y-values.
                False - forces the y-axis to use a predetermined min, max for each
                    data type. Handy when making several plots to be shown together
                    for comparison.
        Returns:
            figure handle - can be used to close the figure and/or save it to a file
    '''

    # Unclear how sensorID was part of the df previously, but it is no longer there; replace this
    senID = '000'  # ToDo: consider removing this completely, or passing it in if it is required
    # senID = df["sensorID"].unique()

    if (verbose or add_suptitle):
        utils_plot_logger.info(f'Sensor: {senID}')
        # print(f'Sensor: {senID}')

    if (ax is None):
        fig, ax = plt.subplots(1, figsize=(10, plot_ht))
    elif (type(ax) is plt_axes._axes.Axes):
        # 'matplotlib.axes._axes.Axes'
        # print('passed in ax which is already matplotlib.axes._axes.Axes')
        fig = ax.get_figure()
    else:
        utils_plot_logger.debug(f'passed in ax which is type {type(ax)}')
        # print(f'passed in ax which is type {type(ax)}')
        fig = ax.Axes.get_figure()

    # print(f'fig is type {type(fig)}')
    utils_plot_logger.debug(f'fig is type {type(fig)}')

    color_dict = {
        'purple': '#800080',
        'navy': '#000080',
        'med blue': '#809FFF',
        'light blue': '#BFCFFF',
        'green': '#008000',
        'yellow': '#008000',
        'orange': '#FFA600',
        'red': '#FF0000',
        'brown': '#800000',
        'gray': '#BFBFBF'
    }

    chan_dict = {  # these may be wrong... only the hex colors match the named colors for sure
        'lch0': {'color': 'purple', 'label': '415 nm'},  # f1 - 415 nm  purple
        'lch1': {'color': 'navy', 'label': '445 nm'},  # f2 - 445 nm  navy
        'lch2': {'color': 'med blue', 'label': '480 nm'},  # f3 - 480 nm  med blue
        'lch3': {'color': 'light blue', 'label': '515 nm'},  # f4 - 515 nm  light blue
        # lch4 and lch5 are not used
        'lch6': {'color': 'green', 'label': '555 nm'},  # f5 - 555 nm  green
        'lch7': {'color': 'yellow', 'label': '590 nm'},  # f6 - 590 nm  yellow
        'lch8': {'color': 'orange', 'label': '630 nm'},  # f7 - 630 nm  orange
        'lch9': {'color': 'red', 'label': '680 nm'},  # f9 - 680 nm  red
        'lch10': {'color': 'gray', 'label': 'All (no filter)'},  # clear - maybe gray or dashed? Si response, non-filtered
        'lch11': {'color': 'brown', 'label': 'NIR 910 nm'},  # NIR - 910 nm  maybe brown?
        'ff': {'color': 'gray', 'label': 'Flicker Hz'}  # flicker (dashed)
    }

    lchans = ['lch0', 'lch1', 'lch2', 'lch3', 'lch6', 'lch7', 'lch8', 'lch9', 'lch11', 'lch10']
    for c in lchans:
        color_to_use = chan_dict[c]['color']
        ret = df.plot.line(x='ts',
                           y=c,
                           ax=ax,
                           color=color_dict[color_to_use],
                           label=chan_dict[c]['label'],
                           #  marker='*',style=True,legend=False
                           )
    # unclear if this works; the idea was to make the face solid white instead of the half-transparent default
    utils_plot_logger.info(type(ret))
    legend = plt.legend()  # Don't need these with the plot titles
    frame = legend.get_frame()
    frame.set_facecolor('white')
    if (not yaxis_adjusts_to_data):
        ax.set_ylim(0, 1)
    ax.tick_params(labelbottom=False)

    if (add_legend is False):
        ax.get_legend().remove()

    if (add_suptitle):
        plt.suptitle(f'Sensor ID {senID}')
    plt.tight_layout()
    return fig


def snapshot(df, plot_ht=4, yaxis_adjusts_to_data=False, verbose=False):
    ''' Builds a set of plots to give a partial snapshot of an EnvSensor data folder
        Args:
            df (dataframe): ES data as a pandas dataframe
            plot_ht (integer): plot height
            yaxis_adjusts_to_data (boolean):
                True - the y-axis zooms to the amount of data. Handy for seeing
                    data with small y-values.
                False - forces the y-axis to use a predetermined min, max for each
                    data type. Handy when making several plots to be shown together
                    for comparison.
        Returns:
            figure handle - can be used to close the figure and/or save it to a file
    '''
    # cols = ['ts', 'lch0', 'lch1', 'lch2', 'lch3', 'lch6', 'lch7', 'lch8', 'lch9',
    #         'lch10', 'lch11', 'pm1', 'pm2.5', 'pm4', 'pm10', 'hum', 'temp', 'voc',
    #         'nox', 'screen', 'ff', 'inttemp', 'csv_name', 'pID', 'sensorID']

    fig, ax = plt.subplots(6, 1, figsize=(10, 4 * plot_ht))
    utils_plot_logger.info(f'ax shape is {ax.shape}')
    # print(f'ax shape is {ax.shape}')

    fig.suptitle('Snapshot\n\n')

    df.plot.line(x='ts', y='pm2.5', ax=ax[0], legend=False)
    if (yaxis_adjusts_to_data):
        ax[0].set_title('PM2.5 - yaxis scales with data')
    else:
        ax[0].set_title('PM2.5 - yaxis [0, 7000]')
        ax[0].set_ylim(0, 7000)
    ax[0].tick_params(labelbottom=False)
    x_axis_0 = ax[0].axes.get_xaxis()
    x_label_0 = x_axis_0.get_label()
    x_label_0.set_visible(False)

    df.plot.line(x='ts', y='voc', ax=ax[1], legend=False)
    if (yaxis_adjusts_to_data):
        ax[1].set_title('VOC - yaxis scales with data')
    else:
        ax[1].set_title('VOC - yaxis [0, 505]')
        ax[1].set_ylim(0, 505)
    ax[1].tick_params(labelbottom=False)
    x_axis_1 = ax[1].axes.get_xaxis()
    x_label_1 = x_axis_1.get_label()
    x_label_1.set_visible(False)

    df.plot.line(x='ts', y='nox', ax=ax[2], legend=False)
    if (yaxis_adjusts_to_data):
        ax[2].set_title('NOx - yaxis scales with data')
    else:
        ax[2].set_title('NOx - yaxis set [0, 50]')
        ax[2].set_ylim(0, 50)
    ax[2].tick_params(labelbottom=False)
    x_axis_2 = ax[2].axes.get_xaxis()
    x_label_2 = x_axis_2.get_label()
    x_label_2.set_visible(False)

    df.plot.line(x='ts', y='temp', ax=ax[3], legend=False)
    if (yaxis_adjusts_to_data):
        ax[3].set_title('Temperature [C] - yaxis scales with data')
    else:
        ax[3].set_title('Temperature [C] - yaxis set [15, 60]')
        ax[3].set_ylim(15, 60)
    ax[3].tick_params(labelbottom=False)
    x_axis_3 = ax[3].axes.get_xaxis()
    x_label_3 = x_axis_3.get_label()
    x_label_3.set_visible(False)

    df.plot.line(x='ts', y='hum', ax=ax[4], legend=False)
    if (yaxis_adjusts_to_data):
        ax[4].set_title('Relative Humidity - yaxis scales with data')
    else:
        ax[4].set_title('Relative Humidity - yaxis set [0, 100]')
        ax[4].set_ylim(0, 100)
    ax[4].tick_params(labelbottom=False)
    x_axis_4 = ax[4].axes.get_xaxis()
    x_label_4 = x_axis_4.get_label()
    x_label_4.set_visible(False)
    ax[4].tick_params(axis='x', bottom=False)

    _ = create_spectral_plot(df, plot_ht=4, ax=ax[5],
                             yaxis_adjusts_to_data=yaxis_adjusts_to_data,
                             verbose=True)
    if (yaxis_adjusts_to_data):
        ax[5].set_title('Spectrum - yaxis scales with data')
    else:
        ax[5].set_title('Spectrum - yaxis set [0, 1]')
        ax[5].set_ylim(0, 1)

    ax[5].tick_params(labelbottom=False)
    x_axis_5 = ax[5].axes.get_xaxis()

    # Working on getting date ticks for the bottom plot only
    time_span = df['ts'].max() - df['ts'].min()
    time_span_days = time_span.days

    if (time_span_days > 1):  # only do 12 hr tick spacing if >> 12 hrs in plot
        if (verbose):
            print('DEBUG .. snapshot date tick interval start')
        ax5 = ax[5]
        ax5.xaxis.set_major_locator(mdates.HourLocator(byhour=12))  # each day @ noon
        ax5.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

    x_label_5 = x_axis_5.get_label()
    x_label_5.set_visible(True)

    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=True,       # set ticks along the bottom edge on
        top=False,         # set ticks along the top edge off
        labelbottom=True)  # set labels along the bottom edge on

    plt.xlabel('timestamp')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    return fig


def get_datetime_from_timestr(t_str):
    '''Given the string timestamp in a *.csv file, return a datetime object
        Example usage:
        fdate_obj = datetime_from_timestampstr('2023-11-18 00:19:23')
    '''
    datetime_object = datetime.strptime(t_str, '%Y-%m-%d %H:%M:%S')  # 4 digit Year
    return datetime_object


def plot_time_and_spectral(df, visit_date, return_date, plot_ht=4, yaxis_adjusts_to_data=False):
    '''From a dataframe of all channels, plot only timestamp and light spectrum vs. timestamp
        This plot provides a visual method to check the time window included
        Args:
            df (dataframe): ES data as a pandas dataframe
            visit_date (string): participant visit date and start of data collection
            return_date (string): participant device return date
            plot_ht (integer): plot height
            yaxis_adjusts_to_data (boolean):
                True - the y-axis zooms to the amount of data. Handy for seeing
                    data with small y-values.
                False - forces the y-axis to use a predetermined min, max for each
                    data type. Handy when making several plots to be shown together
                    for comparison.
        Returns:
            figure handle - can be used to close the figure and/or save it to a file

        yaxis_adjusts_to_data setting only applies to spectral
    '''
    # cols = ['ts', 'lch0', 'lch1', 'lch2', 'lch3', 'lch6', 'lch7', 'lch8', 'lch9',
    #   'lch10', 'lch11', 'pm1', 'pm2.5', 'pm4', 'pm10', 'hum', 'temp', 'voc',
    #   'nox', 'screen', 'ff', 'inttemp', 'csv_name', 'pID', 'sensorID']

    fig, ax = plt.subplots(2, 1, figsize=(10, 4 * plot_ht))
    utils_plot_logger.info(f'ax shape is {ax.shape}')

    def adjust_datetime_from_date(date_string, offset_hours=12):
        # no hour in date_string, so corresponds to midnight
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d') + \
            timedelta(hours=offset_hours)
        return datetime_object
    # Easternmost time zone is Central, which is (UTC −06:00, DST UTC −05:00)
    # Westernmost time zone is Pacific, which is (UTC -08:00, DST UTC - 7:00)
    # settings below correspond to  at least midnight before the visit
    # and midnight after
    start_time = adjust_datetime_from_date(visit_date, offset_hours=6)
    end_time = adjust_datetime_from_date(return_date, offset_hours=32)

    fig.suptitle(f'Time_shot - quality check\nx-axis {start_time} to {end_time}\n')

    df.plot.scatter(x='ts', y='ts', ax=ax[0], legend=False)

    ax[0].set_title(f'Scatter time - expect {visit_date} - {return_date}')

    ax[0].tick_params(labelbottom=False)
    x_axis_0 = ax[0].axes.get_xaxis()
    x_label_0 = x_axis_0.get_label()
    x_label_0.set_visible(False)

    _ = create_spectral_plot(df, plot_ht=4, ax=ax[1], verbose=True)
    if (yaxis_adjusts_to_data):
        ax[1].set_title('Spectrum - yaxis scales with data')
    else:
        ax[1].set_title('Spectrum - yaxis set [0, 1]')
        ax[1].set_ylim(0, 1)
    # constrain x-axis to visit_date - return_date range

    ax[0].set_xlim(start_time, end_time)
    ax[1].set_xlim(start_time, end_time)

    # Working on getting date ticks for the bottom plot only
    if (0):  # original
        ax[1].tick_params(labelbottom=False)
        x_axis_5 = ax[1].axes.get_xaxis()
        x_label_5 = x_axis_5.get_label()
        x_label_5.set_visible(False)
    else:  # experiments
        print('DEBUG: setting date ticks at 12h intervals')
        ax[1].tick_params(labelbottom=True)
        x_axis_5 = ax[1].axes.get_xaxis()

        ax5 = ax[1]
        ax5.xaxis.set_major_locator(mdates.HourLocator(byhour=12))  # looks good, unclear what part of day
        ax5.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

        x_label_5 = x_axis_5.get_label()
        x_label_5.set_visible(True)

    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=True,       # set ticks along the bottom edge on
        top=False,         # set ticks along the top edge off
        labelbottom=True)  # set labels along the bottom edge on

    plt.xlabel('timestamp')
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()

    return fig


def plot_particles(df, visit_date, return_date, plot_ht=4, yaxis_adjusts_to_data=False):
    ''' Builds a single figure with a set of plots to give a partial snapshot of an EnvSensor data folder
        Args:
            df (dataframe): ES data as a pandas dataframe
            visit_date (string): participant visit date and start of data collection
            return_date (string): participant device return date
            plot_ht (integer): plot height
            yaxis_adjusts_to_data (boolean):
                True - the y-axis zooms to the amount of data. Handy for seeing
                    data with small y-values.
                False - forces the y-axis to use a predetermined min, max for each
                    data type. Handy when making several plots to be shown together
                    for comparison.
        Returns:
            figure handle - can be used to close the figure and/or save it to a file
    '''
    # cols = ['ts', 'lch0', 'lch1', 'lch2', 'lch3', 'lch6', 'lch7', 'lch8', 'lch9',
    #         'lch10', 'lch11', 'pm1', 'pm2.5', 'pm4', 'pm10', 'hum', 'temp', 'voc',
    #         'nox', 'screen', 'ff', 'inttemp', 'csv_name', 'pID', 'sensorID']

    fig, ax = plt.subplots(4, 1, figsize=(10, 4 * plot_ht))
    utils_plot_logger.info(f'ax shape is {ax.shape}')
    # print(f'ax shape is {ax.shape}')

    fig.suptitle('Particles\nVisit {vist_date}   Return {return_date}\n')

    df.plot.line(x='ts', y='pm1', ax=ax[0], legend=False)
    if (yaxis_adjusts_to_data):
        ax[0].set_title('PM1 - yaxis scales with data')
    else:
        ax[0].set_title('PM1 - yaxis [0, 7000]')
        ax[0].set_ylim(0, 7000)
    ax[0].tick_params(labelbottom=False)
    x_axis_0 = ax[0].axes.get_xaxis()
    x_label_0 = x_axis_0.get_label()
    x_label_0.set_visible(False)

    df.plot.line(x='ts', y='pm2.5', ax=ax[1], legend=False)
    if (yaxis_adjusts_to_data):
        ax[1].set_title('PM2.5 - yaxis scales with data')
    else:
        ax[1].set_title('PM2.5 - yaxis [0, 7000]')
        ax[1].set_ylim(0, 7000)
    ax[1].tick_params(labelbottom=False)
    x_axis_1 = ax[1].axes.get_xaxis()
    x_label_1 = x_axis_1.get_label()
    x_label_1.set_visible(False)

    df.plot.line(x='ts', y='pm4', ax=ax[2], legend=False)
    if (yaxis_adjusts_to_data):
        ax[2].set_title('PM4 - yaxis scales with data')
    else:
        ax[2].set_title('PM4 - yaxis [0, 7000]')
        ax[2].set_ylim(0, 7000)
    ax[2].tick_params(labelbottom=False)
    x_axis_2 = ax[2].axes.get_xaxis()
    x_label_2 = x_axis_2.get_label()
    x_label_2.set_visible(False)

    df.plot.line(x='ts', y='pm10', ax=ax[3], legend=False)
    if (yaxis_adjusts_to_data):
        ax[3].set_title('PM10 - yaxis scales with data')
    else:
        ax[3].set_title('PM10 - yaxis [0, 7000]')
        ax[3].set_ylim(0, 7000)
    ax[3].tick_params(labelbottom=False)
    x_axis_3 = ax[3].axes.get_xaxis()
    x_label_3 = x_axis_3.get_label()
    x_label_3.set_visible(False)

    # Working on getting date ticks for the bottom plot only
    ax[3].tick_params(labelbottom=True)
    x_axis_3 = ax[3].axes.get_xaxis()

    time_span = df['ts'].max() - df['ts'].min()
    time_span_days = time_span.days

    if (time_span_days > 1):  # only do 12 hr tick spacing if >> 12 hrs in plot
        ax3 = ax[3]
        ax3.xaxis.set_major_locator(mdates.HourLocator(byhour=12))  # each day @ noon
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

    x_label_3 = x_axis_3.get_label()
    x_label_3.set_visible(True)

    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=True,       # set ticks along the bottom edge on
        top=False,         # set ticks along the top edge off
        labelbottom=True)  # set labels along the bottom edge on

    plt.xlabel('timestamp')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    return fig


def dataplot(conv_dict, output_folder):

    input_csv_file = conv_dict['output_file']
    dataplot_dict = dict()

    utils_plot_logger.info(f'metadata input_csv_file {input_csv_file}')

    skiprows = read_skiprow_count_from_file(input_csv_file)

    try:
        df = pd.read_csv(input_csv_file, skiprows=skiprows, index_col='ts')
    except Exception as e:  # FileNotFoundError is most likely
        utils_plot_logger.error(f'Exception in dataplot {e} for {input_csv_file}')
        return dataplot_dict

    fpath_input = conv_dict['output_file']

    df = pd.read_csv(fpath_input, skiprows=skiprows, index_col=None)
    df['ts'] = df.apply(lambda x: get_datetime_from_timestr(x['ts']), axis=1)  # convert to timestamp

    fig = snapshot(df, plot_ht=4, yaxis_adjusts_to_data=False)

    plname = conv_dict['r']['pppp'] + '_snapshot.png'
    plotfile_name = output_folder + '/' + plname
    print(f'Save plot is next; output_file is {plotfile_name}')
    try:
        fig.savefig(plotfile_name)
        dataplot_dict['output_file'] = plotfile_name
        print(f'Success; output_file is {plotfile_name}')
        plt.close()
    except Exception as e:
        utils_plot_logger.error(f'Exception {e} when writing {plotfile_name}')
        dataplot_dict['output_file'] = 'None'

    return dataplot_dict
