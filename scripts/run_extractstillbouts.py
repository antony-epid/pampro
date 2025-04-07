# pampro-manager-processing-scripts - processing scripts for the pampro pipeline managed by 'pampro-manager'
# Copyright (C) 2019  MRC Epidemiology Unit, University of Cambridge
#   
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.
#   
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

## Script to extract still bouts and write them to an HDF5 file
## pampro processing pipeline 1

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
from pampro import data_loading, hdf5, batch_processing, batch_processing_hpc, batch_processing_future, triaxial_calibration, pampro_fourier, Bout

today = datetime.now().strftime('%d%b%Y')

#######################################################################################################################

def files_to_process(settings):
    func_name = "extractstillbouts"
    hdf5_folder = settings.get("hdf5_folder")[0]
    results_folder = settings.get("results_folder")[0]
    log_folder = settings.get("logs_folder")[0]
    ext = settings.get("raw_file_extension")[0]
    logfunc = glob.glob(log_folder + '/' + '*' + func_name  + '*.csv')
    logfunc = [os.path.basename(file.split('_' + func_name + '_')[0]) for file in logfunc]
    logfunc = set(logfunc)

    target_freq = str(settings.get("target_frequency")[0])
    delfreq= "_{}Hz".format(target_freq)

    hdf5files = os.listdir(hdf5_folder)
    inpfiles = [file.strip().split('.')[0].replace(delfreq,'') for file in hdf5files] #hdf5 filename
    inpfiles = set(inpfiles)

    ofiles = list(inpfiles - logfunc)



def extractstillbouts(job_details, settings):

    hdf5_filename = str(job_details["hdf5_filename"])
    monitor = str(job_details["monitor"])

    stillbouts_folder = settings.get("stillbouts_folder")[0]
    noise_cutoff_mg = settings.get("noise_cutoff_mg")[0]
    temperature_calibration = settings.get("temperature_calibration")[0].strip("()'',")

    ts, header = data_loading.load(hdf5_filename, "HDF5", hdf5_mode="r+", hdf5_group=("Resampled"))

    x, y, z, temperature = ts.get_channels(["X", "Y", "Z", "Temperature"])

    id_num = header["subject_code"]
    start = (header["start"]).split(" ")[0]
    wear_date = datetime.strptime(start, '%d/%m/%Y').strftime('%d%b%Y')

    # Create a time series and header of the still bouts information
    
    stillbouts_ts, stillbouts_header = triaxial_calibration.calibrate_stepone(x, y, z, temperature, noise_cutoff_mg=noise_cutoff_mg)
        
    stillbouts_header["sampling_frequency"] = header["frequency"]

    # Create hdf5 file of still bout Time Series object for file
    hdf5_stillbouts = os.path.join(stillbouts_folder, "{}_{}_{}.hdf5".format(monitor, id_num, wear_date))
    hdf5.save(stillbouts_ts, hdf5_stillbouts, file_header=stillbouts_header, groups=[("Still", ["X_n", "X_mean", "Y_mean", "Z_mean", "X_std", "Y_std", "Z_std", "Temperature_mean", "Temperature_std"])], meta_candidates=[])

    for c in stillbouts_ts:
        del c.data
        del c.timestamps
        del c
    del stillbouts_ts

    ## change group and permissions of files
    #os.system("chgrp {} {} & chmod 770 {}".format(group, hdf5_stillbouts, hdf5_stillbouts))

    return {"still_bouts_file": hdf5_stillbouts, "monitor_num": monitor}

#######################################################################################################################


# # parse config file
# settings = pd.read_csv(settings_file, dtype=str)

# # parse jobs list file
# jobs_df = pd.read_csv(jobs_file, dtype=str)

# batch_processing_hpc.batch_process_wrapper(extractstillbouts, jobs_df, settings, job_num, num_jobs, nprocs)


if __name__ == "__main__":
    # print the time taken to run the script
    start_time = time.time()
    print("Script started at: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))    

    settings_file = str(sys.argv[1])
    jobs_file = str(sys.argv[2])

    # parse config file
    settings = pd.read_csv(settings_file, dtype=str)



