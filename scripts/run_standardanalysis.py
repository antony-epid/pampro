# pampro-manager-processing-scripts - processing scripts for the pampro pipeline managed by 'pampro-manager'
# Copyright (C) 2019  MRC Epidemiology Unit, University of Cambridge
#   
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.
#   
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

## Script to calibrate data and produce statistical results
## pampro processing pipeline 1

from datetime import timedelta, datetime
import sys, time
import os
import json
from pampro import data_loading, channel_inference, batch_processing,  batch_processing_hpc, batch_processing_future, triaxial_calibration, time_utilities, pampro_utilities, pampro_fourier, Bout, Channel
import pandas as pd
import numpy as np
from glob import glob

#######################################################################################################################

def files_to_process(settings):

    func_name = "standardanalysis"
    log_folder = settings.get("logs_folder")[0]
    logfunc = glob.glob(log_folder + '/' + '*' + func_name  + '*.csv')
    logfunc = [os.path.basename(file.split('_' + func_name + '_')[0]) for file in logfunc]
    logfunc = set(logfunc)

    hdf5_folder = settings.get("hdf5_folder")[0]
    target_freq = str(settings.get("target_frequency")[0])
    delfreq= "_{}Hz".format(target_freq)
    hdf5files = os.listdir(hdf5_folder)
    inpfiles = [file.strip().split('.')[0].replace(delfreq,'') for file in hdf5files] #hdf5 filename
    inpfiles = set(inpfiles)

    ofiles = list(inpfiles - logfunc)

    dictclb = {} # dictionary of calibrate dataframe (one row with the value from **calibrate**log**successful**csv)
    for file in glob.glob(os.path.join(log_folder,'*calibratemonitor*')):
      df = pd.read_csv(file)
      #the following line is added to deal with the situation when the calibrate monitor log contain more than one rows which are caused by the update which may be wanted when more raw files become available or have not been previously processed.
      #the latest seems to be added into the first row
      if df.shape[0] > 1:
         df = df.iloc[0:1] 
      monitor = df['monitor'][0]#without index [0], the result will be a Series
      dictclb[monitor] = df[df.columns.difference(['monitor'])]#make monitor id as the dictionary key
      dictclb[monitor]['calibration_date']=datetime.fromtimestamp(os.path.getmtime(file)) # add one more variable to the dict

    return ofiles, dictclb


def get_monitor_from_qc(qcfilename):
    store = pd.read_csv(qcfilename, dtype=str)
    monid = store['device'][0]
    return monid

#def standardanalysis(job_details, settings):
def standardanalysis(settings, filename, pid, dictcalib):

    # number of iterations to be used when optimising during calibration
    num_iterations = 500
    
    # threshold of end error for data validity
    end_error_threshold = 10
    
    results_folder = settings.get("results_folder")[0]
    plots_folder = settings.get("plots_folder")[0]
    noise_cutoff_mg = settings.get("noise_cutoff_mg")[0]
    processing_epoch = settings.get("processing_epoch")[0]
    epochs = settings.get("epochs")[0]
    collapse = settings.get("collapse_data")[0].strip("()'',")
    whole_file = settings.get("whole_file")[0].strip("()'',")
    temperature_calibration = settings.get("temperature_calibration")[0].strip("()'',")

    hdf5_folder = settings.get("hdf5_folder")[0]

    target_freq = str(settings.get("target_frequency")[0])
    delfreq= "_{}Hz".format(target_freq)

    qcfile = os.path.join(results_folder, "qc_meta_" + filename + ".csv")
    monitor = get_monitor_from_qc(qcfile)

    job_details = dictcalib[monitor]

    # extract the multi-file calibration errors from the job details
    mf_start_error = job_details["start_error"]
    mf_end_error = job_details["end_error"]

    # generate the epochs dictionary
    epoch_dict = pampro_utilities.json_epochs_to_dict(epochs)

    # generate the list of cut points
    intensities = settings.get("cutpoints")[0]
    intensities_list = pampro_utilities.json_cutpoints_to_list(intensities)

    # generate the list of angles
    angles = settings.get("angles")[0]
    angles_list = pampro_utilities.json_cutpoints_to_list(angles)

    # generate a list of stats that have been selected (e.g. have value == 1)
    stats_list = ['integrity']
    for stat_name in ['enmo', 'hpfvm', 'pitch', 'roll', 'temperature', 'battery']:
        if settings.get(stat_name)[0] == '1':
            stats_list.append(stat_name)

    stats = pampro_utilities.define_statistics(stats_list, intensities_list, angles_list)

    # find which time increments (epochs), if any, are to be plotted
    epochs_plot = settings.get("epochs_plot")[0]
    
    # create a list of increments to be used later
    plot_inc = []
    
    # if epochs_plot is empty, leave plotting list empty
    if epochs_plot is np.nan:
        pass

    else:
        incs = json.loads(epochs_plot)
        for i in incs:
            name = i["name"]
            if i["plot"] == 1:
                plot_inc.append(name)
    
    # find which statistics are to be plotted, and create a dictionary to be used later
    plotting_dict = dict()
    for var, plot_name in zip(["enmo_plot", "hpfvm_plot", "pitch_plot", "roll_plot", "temperature_plot", "battery_plot"],["ENMO_sum", "HPFVM_sum", "PITCH_mean", "ROLL_mean", "Temperature_mean", "Battery_mean"]):
        # only add to plotting dict if selected for processing, i.e. is in stats_list
        if settings.get(var)[0] == '1' and (var.split("_")[0]) in stats_list:
            plotting_dict[plot_name] = "{}_{}_" + plot_name + ".png"
    
    # get job details
    #pid = str(pid)    
    #filename = str(job_details["filename"])
    hdf5_filename = os.path.join(hdf5_folder,filename + delfreq + '.hdf5')

    analysis_meta = os.path.join(results_folder, "analysis_meta_{}.csv".format(filename))
    # check if analysis_meta already exists...
    if os.path.isfile(analysis_meta):
        os.remove(analysis_meta)
        
    # Load the data from the hdf5 file
    ts, header = data_loading.load(hdf5_filename, "HDF5", hdf5_mode="r+", hdf5_group=("Resampled"))

    # some monitors have manufacturers parameters applied to them, let's preserve these but rename them:
    var_list = ["x_gain", "x_offset", "y_gain", "y_offset", "z_gain", "z_offset", "calibration_date"] 
    for var in var_list:
        if var in header.keys():
            header[("manufacturers_%s" % var)] = header[var]
            header.pop(var)

    x, y, z, temperature, battery, integrity = ts.get_channels(["X", "Y", "Z", "Temperature", "Battery", "Integrity"])

    # Calculate the start and end points to use, depending on whether the whole file is required
    if whole_file == "YES":
        start = time_utilities.start_of_day(x.timeframe[0])
        end = time_utilities.end_of_day(x.timeframe[-1])
        # calculate the time period to use
        tp = (start, end)
        num_days = (end - start).days

    else:
        num_days = settings.get("days_of_data")[0].strip("(),")
        start = time_utilities.start_of_day(x.timeframe[0])
        end = start + timedelta(days=int(num_days))
        # calculate the time period to use
        tp = (start, end)

    # check the integrity channel for "1", which shows that anomalies have been fixed BEFORE applying calibration factors
    if 1 in integrity.data:
        # extract the bouts of the integrity channel where the data == 1 (the "integrity compromised" value)
        exclusion_bouts = integrity.bouts(1, 1)
    else:
        exclusion_bouts = []
    
    epoch_list = []
    for e in epoch_dict.keys():
        epoch_list.append(e)
    
    ################ CALIBRATION #######################
    
    ## FIRST CALIBRATE FILE INDIVIDUALLY
    
    # extracting the still bouts from the data
    if temperature_calibration == "YES":
        calibration_ts, calibration_header = triaxial_calibration.calibrate_stepone(x, y, z, temperature, noise_cutoff_mg=noise_cutoff_mg)
    else:
        calibration_ts, calibration_header = triaxial_calibration.calibrate_stepone(x, y, z, noise_cutoff_mg=noise_cutoff_mg)
    
    # Calibrate the acceleration to local gravity
    file_cal_diagnostics = triaxial_calibration.calibrate_steptwo(calibration_ts, calibration_header, calibration_statistics=False, num_iterations=num_iterations)
    
    # extract the file-level calibration errors from the calibration_results
    file_start_error = file_cal_diagnostics["start_error"]
    file_end_error = file_cal_diagnostics["end_error"]
    
    ## SECOND EXAMINE FILE END ERROR 
    
    # compare individual file error to error threshold
    if file_end_error < end_error_threshold:
        calibration_type = "single"
        
        # calibrate the acceleration to local gravity using calibration factors derived from the file itself:
        if temperature_calibration == "YES":
            try:
                triaxial_calibration.do_calibration(x, y, z, temperature, file_cal_diagnostics)
            # catch if ValueError occurs, if it's to do with broadcasting arrays, calibrate without using temperature
            except ValueError as Argument:
                assert (str(Argument).startswith("operands could not be broadcast together")), "different type of ValueError; not a broadcasting error"
                triaxial_calibration.do_calibration(x, y, z, temperature=None, cp=file_cal_diagnostics)
                header["calibration_temperature_fail"] = "True"
        else:
            triaxial_calibration.do_calibration(x, y, z, temperature=None, cp=file_cal_diagnostics)
    
        metadata = {**header, **file_cal_diagnostics}
        for key in ["start_error", "end_error"]:
            metadata[("file_%s" % key)] = metadata[key]
            metadata.pop(key)
    
    else:
        # compare multi-file error to error threshold
        if float(mf_end_error) < end_error_threshold:
            calibration_type = "multi"
        
            # Calibrate the acceleration to local gravity using multi file calibration factors from the calibration database
            calibration_dict = {"x_scale": float(job_details["x_scale"]),
                                "x_offset": float(job_details["x_offset"]),
                                "x_temp_offset": float(job_details["x_temp_offset"]),
                                "y_scale": float(job_details["y_scale"]),
                                "y_offset": float(job_details["y_offset"]),
                                "y_temp_offset": float(job_details["y_temp_offset"]),
                                "z_scale": float(job_details["z_scale"]),
                                "z_offset": float(job_details["z_offset"]),
                                "z_temp_offset": float(job_details["z_temp_offset"])                        
                                }
            
            try:
                triaxial_calibration.do_calibration(x, y, z, temperature, calibration_dict)
            # catch if ValueError occurs, if it's to do with broadcasting arrays, calibrate without using temperature
            except ValueError as Argument:
                assert (str(Argument).startswith("operands could not be broadcast together")), "different type of ValueError; not a broadcasting error"
                triaxial_calibration.do_calibration(x, y, z, temperature=None, cp=calibration_dict)
                header["calibration_temperature_fail"] = "True"

            metadata = {**header, **calibration_dict}
            metadata["calibration_method"] = job_details["calibration_method"]
            cal_files = job_details["files_used"]
            metadata["files_used_in_calibration"] = cal_files
            metadata["number_files_used"] = cal_files.count(".hdf5")
            metadata["calibration_date"] = job_details["calibration_date"]
            
        else:
            # neither calibration type has been successful
            metadata = {**header}
            calibration_type = "fail"
    
    metadata["hdf5_filename"] = hdf5_filename
    metadata["days_of_data_processed"] = num_days
    metadata["processing_epoch"] = processing_epoch
    metadata["analysis_resolutions"] = epoch_list
    metadata["analysis_statistics"] = stats_list
    metadata["noise_cutoff"] = noise_cutoff_mg
    metadata["device"] = job_details["monitor"]
    metadata["mf_start_error"] = mf_start_error
    metadata["mf_end_error"] = mf_end_error
    metadata["file_start_error"] = file_start_error
    metadata["file_end_error"] = file_end_error
    metadata["calibration_type"] = calibration_type

    # Writing out the metadata to a file
    pampro_utilities.dict_write(analysis_meta, pid, metadata)

    ## THIRD ONLY PROCESS IF CALIBRATION TYPE IS NOT "FAIL"
    
    if calibration_type == "fail":
        results_files = None
        charts = None
        
        #os.system("chgrp {} {} & chmod 770 {}".format(group, analysis_meta, analysis_meta))
          
    else:
        results_files = [os.path.join(results_folder, "{}_{}.csv".format(name, filename)) for name in epoch_dict.keys()]
        files = [open(file, "w") for file in results_files]

        # Write the column headers to the created files
        for f in files:
            f.write(pampro_utilities.design_file_header(stats) + "\n")
        
        # delete the exclusion bouts, if any:
        x.delete_windows(exclusion_bouts)
        y.delete_windows(exclusion_bouts)
        z.delete_windows(exclusion_bouts)
        temperature.delete_windows(exclusion_bouts)
        battery.delete_windows(exclusion_bouts)

        # Derive some signal features
        vm = channel_inference.infer_vector_magnitude(x, y, z)
        # delete the exclusion bouts from vm too
        vm.delete_windows(exclusion_bouts)

        if 'hpfvm' in stats_list:
            vm_hpf = channel_inference.infer_vm_hpf(vm)
        else:
            vm_hpf = None

        if 'enmo' in stats_list:
            enmo = channel_inference.infer_enmo(vm)
        else:
            enmo = None

        if 'pitch' in stats_list or 'roll' in stats_list:
            pitch, roll = channel_inference.infer_pitch_roll(x, y, z)
        else:
            pitch = roll = None

        # Infer nonwear and mask those data points in the signal
        nonwear_bouts = channel_inference.infer_nonwear_triaxial(x, y, z, noise_cutoff_mg=noise_cutoff_mg)

        annotation_bouts = []
        for bout in nonwear_bouts:
            # Show non-wear bouts in orange
            bout.draw_properties = {'lw': 0, 'alpha': 0.5, 'facecolor': '#ffb366'}
            annotation_bouts.append(bout)

        if collapse == 'YES':
            for channel, channel_name in zip([enmo, vm_hpf, pitch, roll, temperature, battery], ["ENMO", "HPFVM", "PITCH", "ROLL", "Temperature", "Battery"]):
                if channel_name in stats:
                    # Collapse the sample data to a processing epoch (in seconds) so data is summarised
                    epoch_level_channel = channel.piecewise_statistics(timedelta(seconds=int(processing_epoch)), time_period=tp)[0]
                    epoch_level_channel.name = channel_name
                    if channel_name in ["Temperature", "Battery"]:
                        pass
                    else:
                        epoch_level_channel.delete_windows(nonwear_bouts)
                    epoch_level_channel.delete_windows(exclusion_bouts)
                    ts.add_channel(epoch_level_channel)
            
            # collapse binary integrity channel
            epoch_level_channel = integrity.piecewise_statistics(timedelta(seconds=int(processing_epoch)), statistics=[("binary", ["flag"])], time_period=tp)[0]
            epoch_level_channel.name = "Integrity"
            ts.add_channel(epoch_level_channel)

        else:
            # If do not want to collapse data to epoch level
            for channel, channel_name in zip([enmo, vm_hpf, pitch, roll, temperature, battery],
                                             ["ENMO", "HPFVM", "PITCH", "ROLL", "Temperature", "Battery"]):
                if channel_name in stats:
                    channel.name = channel_name
                    if channel_name in ["Temperature", "Battery"]:
                        pass
                    else:
                        channel.delete_windows(nonwear_bouts)
                    channel.delete_windows(exclusion_bouts)    
                    ts.add_channel(channel)

        # Piecewise output
        charts = []
        # loop through the epochs (time resolutions) for results
        for epoch, name, file in zip(epoch_dict.values(), epoch_dict.keys(), files):

            results_ts = ts.piecewise_statistics(epoch, statistics=stats, time_period=tp, name=pid)
            results_ts.write_channels_to_file(file_target=file)
            file.flush()

            # if epoch is to be plotted:
            if name in plot_inc:
            # for each statistic in the plotting dictionary, produce a plot in the charts folder
                for stat, plot in plotting_dict.items():
                    results_ts[stat].add_annotations(annotation_bouts)
                    results_ts[stat].add_annotations(exclusion_bouts)
                    chart_file = os.path.join(plots_folder, plot.format(filename, name))
                    results_ts.draw([[stat]], file_target=chart_file)
                    charts.append(chart_file)

        all_files = charts + results_files
        
        # change group and permissions of files
        #for f in all_files:
        #    os.system("chgrp {} {} & chmod 770 {}".format(group, f, f))

    for c in ts:
        del c.data
        del c.timestamps
        del c.indices
        del c.cached_indices
    
    
    
    return {"results": results_files, "analysis_meta_file": analysis_meta, "visualisation_file": charts}


#######################################################################################################################


# # parse config file
# settings = pd.read_csv(settings_file, dtype=str)

# # parse jobs list file
# jobs_df = pd.read_csv(jobs_file, dtype=str)

# batch_processing_hpc.batch_process_wrapper(standardanalysis, jobs_df, settings, job_num, num_jobs, nprocs)

if __name__ == "__main__":
    # print the time taken to run the script
    start_time = time.time()
    print("Script started at: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))    

    jobs_file = str(sys.argv[2])
    settings_file = str(sys.argv[1])
    # parse config file
    settings = pd.read_csv(settings_file, dtype=str)

    filenames, dictclb = files_to_process(settings)
    for i, hfile in enumerate(filenames):
        print("Processing file: {}".format(hfile))
        standardanalysis(settings, hfile, i, dictclb)

    print("Script finished at: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("Time taken: {:.2f} seconds".format(time.time() - start_time))

