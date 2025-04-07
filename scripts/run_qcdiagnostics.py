# pampro-manager-processing-scripts - processing scripts for the pampro pipeline managed by 'pampro-manager'
# Copyright (C) 2019  MRC Epidemiology Unit, University of Cambridge
#   
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.
#   
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

## Script to perform a quality check of data
## pampro processing pipeline 1

import numpy as np
from datetime import datetime, timedelta
import time
import sys, os
from pampro import data_loading, diagnostics, Time_Series, Channel, hdf5, channel_inference, Bout, Bout_Collection, batch_processing, triaxial_calibration, time_utilities, pampro_utilities, pampro_fourier
from collections import OrderedDict
import pandas as pd
from glob import glob

#######################################################################################################################

# PROCESSING SETTINGS AND THRESHOLDS
discharge_hours = 24            # number of hours for the examination of the battery discharge rate
discharge_pct = 25              # maximum percentage of battery charge that can be discharged in "discharge_hours" without warning flag
battery_minimum = 10            # minimum percentage of battery charge, below which warning flag is triggered
GA_battery_max = 4.3            # maximum value of GeneActiv battery, used to find percentage charged
AX_battery_max = 210            # maximum value of Axivity battery, used to find percentage charged
axis_max = 1.2                  # upper threshold for each acceleration axis, above which there is an anomaly on that axis
axis_min = -1.2                 # lower threshold for each acceleration axis, below which there is an anomaly on that axis
still_mins = 10                    # window size (in minutes) to detect axis shift when still

anomaly_types = ["A", "B", "C", "D", "E", "F", "G"]     # A list of known anomaly types identified by pampro

#########################################################################################################################


def files_to_process(settings):
    func_name = "qcdiagnostics"
    log_folder = settings.get("logs_folder")[0]
    data_folder = settings.get("raw_data_folder")[0]
    ext = settings.get("raw_file_extension")[0]
    logfunc = glob.glob(log_folder + '/' + '*' + func_name  + '*.csv')
    logfunc = [os.path.basename(file.split('_' + func_name + '_')[0]) for file in logfunc]
    logfunc = set(logfunc)
    datafiles = glob.glob(data_folder + '/*' + ext) #raw file *.cwa filename
    inpfiles = set(datafiles)
    ofiles = list(inpfiles - logfunc)

    #folderconf = os.path.join(rootdir, config_folder)
    config_folder = settings.get("config_folder")[0]
    if os.path.isdir(config_folder):
        pass
    else:
        os.makedirs(config_folder, exist_ok=True)

    if len(ofiles) > 0:
         ofiles = [os.path.join(data_folder, x + ext) for x in ofiles]
    else:
      print('No more files to process !')
      ofiles = None

    return ofiles


def qcdiagnostics(settings, filename, pid):

    pid = str(pid)
    filename = str(filename)
    filename_short = os.path.basename(filename).split('.')[0]

    monitor_type = settings.get("monitor_type")[0]
    results_folder = settings.get("results_folder")[0]
    plots_folder = settings.get("plots_folder")[0]
    anomalies_folder = settings.get("anomalies_folder")[0]
    noise_cutoff_mg = settings.get("noise_cutoff_mg")[0]
    
    battery_max = 0
    if monitor_type == "GeneActiv":
        battery_max = GA_battery_max
    elif monitor_type == "Axivity":
        battery_max = AX_battery_max

    # create a dataframe of the channels, channel minimum and maximum values to be plotted
    channels_info = {'channel_name': ["X", "Y", "Z", "Battery_percentage"],
                 'channel_min': [-8, -8, -8, 0],
                 'channel_max': [8, 8, 8, 100]}
    plotting_df = pd.DataFrame.from_dict(channels_info)

    ts, header = data_loading.fast_load(filename, monitor_type)

    x, y, z, battery, temperature, integrity = ts.get_channels(["X", "Y", "Z", "Battery", "Temperature", "Integrity"])
    
    # create a channel of battery percentage, based on the assumed battery maximum value 
    battery_pct = Channel.Channel.clone(battery)
    battery_pct.data = (battery.data / battery_max) * 100
    
    channels = [x, y, z, battery, temperature, integrity, battery_pct]

    anomalies = diagnostics.diagnose_fix_anomalies(channels, discrepancy_threshold=2)

    # create dictionary of timestamp anomalies types
    anomalies_dict = dict()
                        
    # check whether any timestamp anomalies have been found:
    if len(anomalies) > 0:
        anomalies_file = os.path.join(anomalies_folder, "{}_anomalies.csv".format(filename_short))
        df = pd.DataFrame(anomalies)
        
        for type in anomaly_types:
            anomalies_dict["QC_anomaly_{}".format(type)] = (df.anomaly_type.values == type).sum()
        
        df = df.set_index("anomaly_type")
        # print record of timestamp anomalies to anomalies_file
        df.to_csv(anomalies_file)
        
    else:
        for type in anomaly_types:
            anomalies_dict["QC_anomaly_{}".format(type)] = 0
        anomalies_file = None
        
    # extract the maximum and minimum axis values when monitor is "still"
    axes_dict = diagnostics.diagnose_axes(x, y, z, window_size=timedelta(minutes=still_mins), noise_cutoff_mg=noise_cutoff_mg)
    header["QC_still_window_minutes"] = still_mins
    
    # create axis anomaly flag, based on max and min values at still points
    axis_anomaly = False
    
    # check if any max or min exceed diagnostic values
    for key, val in axes_dict.items():
        anomalies_dict["QC_{}".format(key)] = val
        if key.endswith("max"):
            if val > axis_max:
                axis_anomaly = True
        elif key.endswith("min"):
            if val < axis_min:
                axis_anomaly = True

    # Find a diagnostic dynamic range for the monitor
    dynamic_range = diagnostics.find_dynamic_range(header, monitor_type) 
    header["QC_diagnostic_dynamic_range"] = dynamic_range
    
    # check for bouts where the axes may have been stuck at maximum or minimum
    stuck_bouts = diagnostics.diagnose_fix_axes_stuck(x, y, z, integrity, dynamic_range)
    if len(stuck_bouts) > 0:
        lst = []
        for bout in stuck_bouts:
            lst.append(str(bout))
        header["QC_stuck_bouts"] = lst
    
    else:
        header["QC_stuck_bouts"] = -1
    
    # create a "check battery" flag:
    check_battery = False

    # calculate first and last battery percentages
    first_battery_pct = round((battery_pct.data[1]),2)
    last_battery_pct = round((battery_pct.data[-1]),2)
    header["QC_first_battery_pct"] = first_battery_pct
    header["QC_last_battery_pct"] = last_battery_pct
    
    # calculate lowest battery percentage
    # check if battery.pct has a missing_value, exclude those values if they exist
    if battery_pct.missing_value == "None":
        lowest_battery_pct = min(battery_pct.data)
    else:
        test_array = np.delete(battery_pct.data, np.where(battery_pct.data == battery_pct.missing_value))
        lowest_battery_pct = min(test_array)
    
    header["QC_lowest_battery_pct"] = round(lowest_battery_pct,2)
    header["QC_lowest_battery_threshold"] = battery_minimum
        
    # find the maximum battery discharge in any 24hr period:    
    max_discharge = battery_pct.channel_max_decrease(time_period=timedelta(hours=discharge_hours))
    header["QC_max_discharge"] = round(max_discharge, 2)
    header["QC_discharge_time_period"] = "{} hours".format(discharge_hours)
    header["QC_discharge_threshold"] = discharge_pct

    # change flag if lowest battery percentage dips below battery_minimum at any point 
    # OR maximum discharge greater than discharge_pct over time period "hours = discharge_hours"
    if lowest_battery_pct < battery_minimum or max_discharge > discharge_pct:
        check_battery = True
        
    header["QC_check_battery"] = str(check_battery)
    header["QC_axis_anomaly"] = str(axis_anomaly)

    # Derive some signal features
    vm = channel_inference.infer_vector_magnitude(x, y, z)
    enmo = channel_inference.infer_enmo(vm)

    # Infer nonwear
    nonwear_bouts = channel_inference.infer_nonwear_for_qc(x, y, z, noise_cutoff_mg=noise_cutoff_mg)
    # Use nonwear bouts to calculate wear bouts
    wear_bouts = Bout.time_period_minus_bouts(enmo.timeframe, nonwear_bouts)

    # Use wear bouts to calculate the amount of wear time in the file in hours, save to meta data
    total_wear = Bout.total_time(wear_bouts)
    total_seconds_wear = total_wear.total_seconds()
    total_hours_wear = round(total_seconds_wear/3600)
    header["QC_total_hours_wear"] = total_hours_wear

    # Split the enmo channel into lists of bouts for each quadrant:
    ''' quadrant_0 = 00:00 -> 06: 00
        quadrant_1 = 06:00 -> 12: 00
        quadrant_2 = 12:00 -> 18: 00
        quadrant_3 = 18:00 -> 00: 00 '''
    q_0, q_1, q_2, q_3 = channel_inference.create_quadrant_bouts(enmo)

    # calculate the intersection of each set of bouts with wear_bouts, then calculate the wear time in each quadrant.
    sum_quadrant_wear = 0
    for quadrant, name1, name2 in ([q_0, "QC_hours_wear_quadrant_0", "QC_pct_wear_quadrant_0"],
                                   [q_1, "QC_hours_wear_quadrant_1", "QC_pct_wear_quadrant_1"],
                                   [q_2, "QC_hours_wear_quadrant_2", "QC_pct_wear_quadrant_2"],
                                   [q_3, "QC_hours_wear_quadrant_3", "QC_pct_wear_quadrant_3"]):
        quadrant_wear = Bout.bout_list_intersection(quadrant, wear_bouts)
        seconds_wear = Bout.total_time(quadrant_wear).total_seconds()
        hours_wear = round(seconds_wear / 3600)
        header[name1] = hours_wear
        header[name2] = round(((hours_wear / total_hours_wear) * 100), 2)

    # file of metadata from qc process
    qc_output = os.path.join(results_folder, "qc_meta_{}.csv".format(filename_short))
    # check if qc_output already exists...
    if os.path.isfile(qc_output):
        os.remove(qc_output)
    
    metadata = {**header, **anomalies_dict}
    
    # write metadata to file
    pampro_utilities.dict_write(qc_output, pid, metadata)

    # produce plots of any anomalies or battery issues detected:
    if check_battery or axis_anomaly or anomalies_file is not None:
        battery_pct.name = "Battery_percentage"
        ts.add_channel(battery_pct)
        qc_plots = os.path.join(plots_folder,"qc_plots_{}.png".format(filename_short))
        ts.draw_qc(plotting_df, file_target=qc_plots)
        
    else:
        qc_plots = None
    
    # change group and permissions of files
    #for f in [anomalies_file, qc_output, qc_plots]:
    #    if f is not None:
    #        os.system("chgrp {} {} & chmod 660 {}".format(group, f, f))
    
    return {"anomalies_file": anomalies_file, "qc_results": qc_output, "qc_visualisation": qc_plots, "monitor": header["device"]}

#######################################################################################################################

# # parse config file
# settings = pd.read_csv(settings_file, dtype=str)
# # parse jobs list file
# jobs_df = pd.read_csv(jobs_file, dtype=str)

# # initiate batch process
# batch_processing.batch_process_wrapper(qcdiagnostics, jobs_df, settings, job_num, num_jobs)

if __name__ == "__main__":
    # print the time taken to run the script
    start_time = time.time()
    print("Script started at: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))    

    settings_file = str(sys.argv[1])
    # parse config file
    settings = pd.read_csv(settings_file, dtype=str)

    rawfiles = files_to_process(settings)
    for i, rawfile in enumerate(rawfiles):
        print("Processing file: {}".format(rawfile))
        qcdiagnostics(settings, rawfile, i)

    print("Script finished at: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("Time taken: {:.2f} seconds".format(time.time() - start_time))