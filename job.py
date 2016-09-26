from import_util import po2016

from po2016.dag.dot2dag import print_dag_info
from po2016 import run
from po2016.application import Application, init_all_apps, num_apps
from po2016.dag.dot2dag import setup_dag
import sys

class Job(object):

    def __init__(self, job_queue_index, dag_file_path, dag, num_dag_nodes,
        requested_power, requested_time, threshold, start_time = -1,
        available_power = -1, available_nodes = -1, completion_power = -1,
        completion_time = -1, completion_workload = -1, max_nodes = -1, runs = []):
        #resources_consumed = []):
        self.queue_index = int(job_queue_index)
        self.dag_file_path = dag_file_path
        self.dag = dag
        self.num_dag_nodes = num_dag_nodes
        self.requested_power = float(requested_power)
        self.requested_time = float(requested_time)
        self.deadline = float(requested_time)
        self.threshold = float(threshold)

        self.start_time = start_time
        self.available_power = available_power
        self.available_nodes = available_nodes
        self.completion_power = completion_power
        self.completion_time = completion_time
        self.completion_workload = completion_workload
        self.max_nodes = max_nodes
        self.runs = runs
    #def record_resource(start_time, duration, power, num_nodes, workload):
    #    self.resources_consumed.append([float(start_time), float(duration),
    #        float(power), int(num_nodes), float(workload)])

    def update_job(job, available_power, available_nodes, run_end_time = -1,
        run_start_time = -1):

        (start, end, power, workload, nodes) = run.get_runs_stats(job.runs)

        job.start_time = start
        job.completion_time = end

        job.completion_power = power
        job.completion_workload = workload
        job.max_nodes = nodes

        if run_start_time > -1:
            job.start_time = run_start_time
        elif run_end_time > -1 and job.start_time < 0:
            job.start_time = run.get_runs_start_time(job.runs)
        if run_end_time > -1:
            job.completion_time = run_end_time
        elif run_start_time > -1 and job.completion_time < 0:
            job.completion_time = run.get_total_runtime(job.runs)

        job.available_power = available_power
        job.available_nodes = available_nodes

        if job.completion_time < job.start_time:
            print("ERROR: Job completion time ahead of job start time!")
            sys.exit(1)

def setup_jobs(dag_names, policy, requested_powers = [], requested_times = [],
    thresholds = [], app_outdir = 0, app_indir = 0):
    
        
    applications = init_all_apps(num_apps)
    if policy == "POW-Opt":
        applications = reduce_application_space(applications)
        
    job_queue = []
    num_jobs = len(dag_names)
    
    for i in range(0, num_jobs):
        (task_graph, num_dag_nodes) = setup_dag(dag_names[i], applications, num_apps, app_indir)
        requested_power = -1
        requested_time = -1
        threshold = -1
        if len(requested_powers) > 0:
            requested_power = requested_powers[i]
        if len(requested_times) > 0:
            requested_time = requested_times[i]
        if len(thresholds) > 0:
            threshold = thresholds[i]
        job = Job(i, dag_names[i], task_graph, num_dag_nodes, requested_power, requested_time, threshold)
        job_queue.append(job)
        
    return job_queue, applications
    


def print_job(job, debug_mode):
    print("++++++++++++JOB INFO+++++++++++++++")
    print("Job:")
    print("\tqueue_index:", job.queue_index)
    print("\tdag file name:", job.dag_file_path)
    if debug_mode == TRACE:
        print("\tdag:", print_dag_info(job.dag))
    print("\tnumber of DAG nodes:", job.num_dag_nodes)
    print("\trequested power:", job.requested_power)
    print("\trequested time:", job.requested_time)
    print("\tdeadline:", job.deadline)
    print("\tthreshold:", job.threshold)
    print("\ttotal power consumed:", job.completion_power)
    print("\ttotal job duration:", job.completion_time)
    print("\ttotal workload completed:", job.completion_workload)
    print("\tmaximum number of nodes needed:", job.max_nodes)
    if debug_mode == TRACE:
        print("\tlist of resources used:")
        for resources in job.resources_consumed:
            print("\t\tstart time:", resources[0], "\tduration:", resources[1])
            print("\t\t\tpower:", resources[2])
            print("\t\t\tnodes:", resources[3])
            print("\t\t\tworkload:", resources[4])
    print("++++++++END JOB INFO+++++++++++++++")

def process_job_schedules(job_schedules, outdir):
    return 0

