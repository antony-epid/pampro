import sys, os
from datetime import datetime
from pampro.pampro_utilities import *
import traceback

def wrap_task(analysis_function, settings, *args, **kwargs):
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

    # # Using job_num and num_jobs, calculate which files this process should handle
    # job_section = job_indices(job_num, num_jobs, len(jobs_df))
    # my_jobs = jobs_df[job_section[0]:job_section[1]]

    #for n, job in my_jobs.iterrows():

    if "filename" in kwargs:
        filename = kwargs["filename"]
        head, tail = os.path.split(filename)
        job_name = tail.split('.')[0]
        #job_name = kwargs["filename"]

    elif "monitor_id" in kwargs:
        job_name = kwargs["monitor_id"]

    else:
        job_name = "unknown"

    pid = kwargs["pid"]
    job_start_time = datetime.now()

    try:
        output_dict = analysis_function(settings, **kwargs)
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
            #for k, v in job.iteritems():
            for k, v in kwargs.items():                
                error_log.write(str(k) + ": " + str(v) + "\n")
            error_log.write("Exception:" + str(sys.exc_info()) + "\n")
            error_log.write(tb + "\n\n")
            error_log.flush()
