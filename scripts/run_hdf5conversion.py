# pampro-manager-processing-scripts - processing scripts for the pampro pipeline managed by 'pampro-manager'
# Copyright (C) 2019  MRC Epidemiology Unit, University of Cambridge
#   
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.
#   
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

## Script to convert a file to HDF5 format
## pampro processing pipeline 1

import sys
import os
from pampro import data_loading, hdf5, batch_processing, pampro_utilities, Channel, Bout, pampro_fourier, batch_processing_hpc, batch_processing_future
from datetime import datetime, timedelta
import pandas as pd

job_num = int(sys.argv[3])
num_jobs = int(sys.argv[4])
settings_file = str(sys.argv[1])
jobs_file = str(sys.argv[2])
nprocs = sys.argv[5] if len(sys.argv) == 6 else 10


#######################################################################################################################

def hdf5conversion(job_details, settings):

    pid = str(job_details["pid"])
    filename = str(job_details["filename"])
    anomalies_file = str(job_details["anomalies_file"])

    filename_short = os.path.basename(filename).split('.')[0]
    monitor_type = settings.get("monitor_type")[0]
    hdf5_folder = settings.get("hdf5_folder")[0]
    results_folder = settings.get("results_folder")[0]
    target_freq = int(settings.get("target_frequency")[0])
    meta_output = os.path.join(results_folder, "file_meta{}.csv".format(filename_short))
    
    hdf5_filename = os.path.join(hdf5_folder, "{}_{}Hz{}".format(filename_short, target_freq, ".hdf5"))
    
    # check if meta_output already exists...
    if os.path.isfile(meta_output):
        os.remove(meta_output)    
    
    if os.path.isfile(hdf5_filename):
        os.remove(hdf5_filename)

    if os.path.isfile(anomalies_file):
        ts, header = data_loading.load(filename, monitor_type, anomalies_file=anomalies_file)
    else:
        ts, header = data_loading.load(filename, monitor_type)

    # Create a list of channels in the Time Series
    channel_list = []
    for channel in ts.channels:
        channel_list.append(channel.name)

    # extract channels from time series
    x, y, z, temperature, battery = ts.get_channels(["X", "Y", "Z", "Temperature", "Battery"])

    if "Integrity" in channel_list:
        integrity = ts.get_channel("Integrity")
    else:
        integrity = Channel.Channel("Integrity")
        integrity.start = x.start
        integrity.set_contents(np.zeros(len(x.data)), x.timestamps, timestamp_policy="offset")
        integrity.binary_date = True

    # create lists of channels by timestamp policy
    normal_channels = []
    sparse_channels = []    
    for channel in [x, y, z, temperature, battery, integrity]:
        if channel.timestamp_policy == "normal":
            normal_channels.append(channel)
        elif channel.timestamp_policy == "sparse":
            sparse_channels.append(channel)   
    
    ##### RESAMPLE #####
    Channel.resample_normal_channels(normal_channels, target_freq)
    
    Channel.resample_sparse_channels(sparse_channels, target_freq)      
    
    # correct the length of the channels if the normal channels are 1 datapoint longer thatn the sparse channels after resampling:
    if len(normal_channels[0].data) == 1 + len(sparse_channels[0].data):
        # then for each normal channel, drop the last datapoint.
        for channel in normal_channels:
            channel.set_contents(channel.data[:-1], channel.timestamps[:-1], timestamp_policy="offset")
            
    elif len(sparse_channels[0].data) == 1 + len(normal_channels[0].data):
        # then for each normal channel, drop the last datapoint.
        for channel in sparse_channels:
            channel.set_contents(channel.data[:-1], channel.timestamps[:-1], timestamp_policy="offset")        
    
    # check for the "missing_value" in the x channel, which shows that anomalies have been fixed
    exclusion_bouts = []
    if -111 in x.data:
        # extract the bouts of the data channels where the data == -111 (the missing value)
        missing = x.bouts(-111, -111)

        # extend each bout (relative to the length of the bout) on either side to account for the effects of interpolation 
        # to get a new list of bouts
        for item in missing:
            # calculate length of bout, based on values of -111
            length = item.end_timestamp - item.start_timestamp
            buffer = 0
            
            # determine buffer to be applied based on this length
            # bout <= 30 minutes, buffer = 1 minute
            if length.total_seconds() <= 1800:
                buffer = 60
            
            #  30 minutes < bout <= 90 minutes, buffer = 5 minutes
            elif length.total_seconds() > 1800 and length.total_seconds() <= 5400:
                buffer = 300

            # 90 minutes < bout <= 6 hours, buffer = 10 minutes
            elif length.total_seconds() > 5400 and length.total_seconds() <= 21600:
                buffer = 600

            # 6 hours < bout <= 24 hours, buffer = 20 minutes        
            elif length.total_seconds() > 21600 and length.total_seconds() <= 86400:
                buffer = 1200
                
            # bout > 24 hours, buffer = 30 minutes
            elif length.total_seconds() > 86400:
                buffer = 1800
            
            bout_start = max(item.start_timestamp - timedelta(seconds=buffer), x.timeframe[0])
            bout_end = min(item.end_timestamp + timedelta(seconds=buffer), x.timeframe[1])
            
            new_bout = Bout.Bout(start_timestamp=bout_start, end_timestamp=bout_end)
            exclusion_bouts.append(new_bout)
    else:
        pass
    
    for channel, lpf_freq in zip([x, y, z, temperature, battery, integrity], [20, 20, 20, None, None, None]):
        if lpf_freq:
            original_name = channel.name
            # apply a low pass filter
            channel = pampro_fourier.low_pass_filter(channel, lpf_freq, frequency=channel.frequency, order=4)
            # because LPF^ changes the name, we want to override that and return to original name
            channel.name = original_name
            
        if channel.name == "Integrity":
            # set the integrity channel to be filled with ones during exclusion bouts (to flag the integrity of that section)
            channel.fill_windows(exclusion_bouts, fill_value=1)
        
        else:
            # all other channels: mask the data as missing during exclusion bouts
            channel.delete_windows(exclusion_bouts)
    
    if len(exclusion_bouts) > 0:
        exclusion_duration = (Bout.total_time(exclusion_bouts)).total_seconds()
    else:
        exclusion_duration = "-1"
    
    header["resampled_frequency"] = target_freq
    header["exclusion_duration"] = exclusion_duration

    try:
        entries = ('date_of_birth', 'sex', 'height', 'weight', 'subject_notes', 'config_notes')
        for key in entries:
            if key in header:
                del header[key]
    except:
        pass
    
    pampro_utilities.dict_write(meta_output, pid, header)
    
    hdf5.save(ts, hdf5_filename, file_header=header, groups=[("Resampled", ["X", "Y", "Z", "Integrity", "Temperature", "Battery"])], data_type="float32", compression=4)


    #hdf5.save(ts, hdf5_filename, file_header=header, groups=[("Raw", ["X", "Y", "Z", "Integrity"]), ("Page", ["Temperature", "Battery"])], data_type="float32", compression=4)

    for c in ts:
         del c.data
         del c.timestamps
         del c
    del ts

    return {"hdf5_file": hdf5_filename, "file_meta_results": meta_output}

#######################################################################################################################


# parse config file
settings = pd.read_csv(settings_file, dtype=str)

# parse jobs list file
jobs_df = pd.read_csv(jobs_file, dtype=str)

batch_processing_hpc.batch_process_wrapper(hdf5conversion, jobs_df, settings, job_num, num_jobs, nprocs)

