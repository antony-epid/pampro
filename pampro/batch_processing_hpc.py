# pampro - physical activity monitor processing
# Copyright (C) 2019  MRC Epidemiology Unit, University of Cambridge
#   
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.
#   
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

import collections
import math
import numpy as np
import sys, os
import glob
from datetime import datetime
import json
import traceback
import pandas as pd
from .pampro_utilities import *
from itertools import repeat
from multiprocessing import Pool, Lock, Process, Queue, current_process, cpu_count

def batch_process(analysis_function, jobs_spec, job_num=1, num_jobs=1, task=None):
    """
    Wrapper to allow a function to be executed as a batch
    """

    batch_start_time = datetime.now()

    if type(jobs_spec) is str:
        
        # Load the document listing all the files to be processed
        # read in the file
        df = pd.read_csv(jobs_spec, engine="python")
        
    else:
    
        df = jobs_spec

    # Using job_num and num_jobs, calculate which files this process should handle
    job_section = job_indices(job_num, num_jobs, len(df))
    my_jobs = df[job_section[0]:job_section[1]]

    if task is None:
        task = analysis_function.__name__

    error_log = False
    output_log = open("_logs" + os.sep + task + "_output_{}.csv".format(str(job_num)), "w")

    for n, job in my_jobs.iterrows():

        output_log.write("\nJob {}/{}\n".format(n+1, len(my_jobs)))
        for index in job.index:
            output_log.write("{}: {}".format(index, job[index]))
        
        job_start_time = datetime.now()
        output_log.write("\nJob start time: " + str(job_start_time))
        output_log.flush()

        try:
            analysis_function(job)
            
        except:

            tb = traceback.format_exc()

            # Create the error file only if an error has occurred
            if error_log is False:
                error_log = open("_logs" + os.sep + task + "_error_{}.csv".format(str(job_num)), "w")

            print("Exception:" + str(sys.exc_info()))
            print(tb)

            error_log.write("Error log at " + str(datetime.now())+ "\n")
            for k, v in job.iteritems():
                error_log.write(str(k) + ": " + str(v) + "\n")
            error_log.write("Exception:" + str(sys.exc_info()) + "\n")
            error_log.write(tb + "\n\n")
            error_log.flush()

        job_end_time = datetime.now()
        job_duration = job_end_time - job_start_time
        output_log.write("\nJob run time: " + str(job_duration))

        batch_duration = job_end_time - batch_start_time
        batch_remaining = (len(my_jobs)-n)*job_duration
        output_log.write("\nBatch run time: " + str(batch_duration))
        output_log.write("\nTime remaining: " + str(batch_remaining))
        output_log.write("\nPredicted completion time:" + str((batch_remaining + datetime.now())) + "\n")
        output_log.flush()

    batch_end_time = datetime.now()
    batch_duration = batch_end_time - batch_start_time
    output_log.write("\nBatch run time: " + str(batch_duration))
    output_log.flush()
    output_log.close()

    # If everything went smoothly, error_log is False because it was never a file object
    if error_log is not False:
        error_log.close()


def pool_task(job, settings, analysis_function):

    task = analysis_function.__name__
    submission_id = settings.get("submission_id")[0]
    logs_folder = settings.get("logs_folder")[0]
    # archive = os.path.join(logs_folder, "archive")
    output_string = "_completed_"
    error_string = "_unsuccessful_"
     
    print(job)
    print(type(job))
    print('Process-id = ', current_process()) 
    print('Number of processes = ', cpu_count())
    if "filename" in job.index:
        filename = job["filename"]
        head, tail = os.path.split(filename)
        job_name = tail.split('.')[0]

    elif "monitor_id" in job.index:
        job_name = job["monitor_id"]

    else:
        job_name = "unknown"

    pid = job["pid"]
    job_start_time = datetime.now()

    try:
        output_dict = analysis_function(job, settings)
        job_end_time = datetime.now()
        job_duration = job_end_time - job_start_time
        output_dict["job_duration"] = str(job_duration)

        output_log = logs_folder + os.sep + job_name + "_" + task + output_string + submission_id + ".csv"
        dict_write(output_log, pid, output_dict)

    except Exception:

        tb = traceback.format_exc()

	# Create the error file only if an error has occurred
        with open(logs_folder + os.sep + job_name + "_" + task + error_string + submission_id + ".csv", "w") as error_log:

            error_log.write("Error log at " + str(datetime.now()) + "\n")
            for k, v in job.iteritems():
                error_log.write(str(k) + ": " + str(v) + "\n")
            error_log.write("Exception:" + str(sys.exc_info()) + "\n")
            error_log.write(tb + "\n\n")
            error_log.flush()



def batch_process_wrapper(analysis_function, jobs_df, settings, job_num=1, num_jobs=1, nprocs=30):
    """ An updated, condensed version of the above wrapper function that is compatible with 'pampro-manager' interface

        Requires a jobs dataframe that contains a pid for each job, and either a filename or monitor number
        to create a 'job name' to name the output or error logs.

        Also requires a settings dataframe that contains the logs folder path and submission id.
    """

    task = analysis_function.__name__

    submission_id = settings.get("submission_id")[0]
    logs_folder = settings.get("logs_folder")[0]
    # archive = os.path.join(logs_folder, "archive")
    output_string = "_completed_"
    error_string = "_unsuccessful_"

    # Using job_num and num_jobs, calculate which files this process should handle
    job_section = job_indices(job_num, num_jobs, len(jobs_df))
    my_jobs = jobs_df[job_section[0]:job_section[1]]

#--------------------pake yg ini saja------------
#>>> list(zip(['aaa','bbb','ccc'],repeat('opu'),repeat('klo')))
#[('aaa', 'opu', 'klo'), ('bbb', 'opu', 'klo'), ('ccc', 'opu', 'klo')]
#listsr = [j for i,j in df[0:3].iterrows()]
    list_jobs_pandaseries  = [j for i,j in my_jobs.iterrows()] # collecting as a list of pandas series
    print('type(list_jobs_pandaseries[0]) = ',type(list_jobs_pandaseries[0]))
    print('index = ',list_jobs_pandaseries[0].index)
    print('values = ',list_jobs_pandaseries[0].values)

    #with Pool(nprocs) as p:
    with Pool() as p:
       #p.starmap(pool_wrapper,zip(my_jobs,repeat(setting),repeat(analysis_function)))
       p.starmap(pool_task,zip(list_jobs_pandaseries,repeat(settings),repeat(analysis_function)))

       #-----------kalo yg diatas sulit, coba yg dibawah, tapi yg dibawah ini keknya gak bisa karena argumen kedua haruslah iterable-----
       #for n, job in my_jobs.iterrows():
       #    p.map(pool_wrapper, job, settings)
       #yg dibawah ini bisa utk 1 argumen, gak tau kalo lebih dari satu argumen...bs langsung pake analysis_function. Ingat haruspake (n,), gak boleh (n) ato n saja
       #for n, job in my_jobs.iterrows():
       #    multiple_results.append[pool.apply_async(twos_multiple,(n,))]
           #print([res.get(timeout=1) for res in multiple_results])




"""
#-----------------------sampai sini ---------------
    for n, job in my_jobs.iterrows():

        if "filename" in job.index:
            filename = job["filename"]
            head, tail = os.path.split(filename)
            job_name = tail.split('.')[0]

        elif "monitor_id" in job.index:
            job_name = job["monitor_id"]

        else:
            job_name = "unknown"

        pid = job["pid"]
        job_start_time = datetime.now()

        try:
            output_dict = analysis_function(job, settings)
            job_end_time = datetime.now()
            job_duration = job_end_time - job_start_time
            output_dict["job_duration"] = str(job_duration)

            output_log = logs_folder + os.sep + job_name + "_" + task + output_string + submission_id + ".csv"
            dict_write(output_log, pid, output_dict)

        except Exception:

            tb = traceback.format_exc()

            # Create the error file only if an error has occurred
            with open(logs_folder + os.sep + job_name + "_" + task + error_string + submission_id + ".csv", "w") as error_log:

                error_log.write("Error log at " + str(datetime.now()) + "\n")
                for k, v in job.iteritems():
                    error_log.write(str(k) + ": " + str(v) + "\n")
                error_log.write("Exception:" + str(sys.exc_info()) + "\n")
                error_log.write(tb + "\n\n")
                error_log.flush()
"""

def job_indices(n, num_jobs, job_list_size):

    n = n-1

    job_size = math.floor(job_list_size/num_jobs)
    remaining = job_list_size - (num_jobs*job_size)

    start_index = 0
    for i in range(num_jobs):

        end_index = min(job_list_size, start_index + job_size)

        if remaining > 0:
            end_index += 1
            remaining -= 1

        if i == n:
            return start_index, end_index

        start_index = end_index
