from import_util import po2016

from po2016.dag import print_dag_info
from po2016.run import Run
from po2016.application import Application, init_all_apps, num_apps
from po2016.dag.dot2dag import setup_dag
import sys

class Job(object):

    def __init__(self, job_queue_index, dag_file_path, dag, num_dag_nodes, requested_power, requested_time,
        threshold, start_time = 0.0, available_power = 0.0, available_nodes = 0.0, completion_power = 0.0, completion_time = 0.0,
        completion_workload = 0.0, max_nodes = 0, resources_consumed = []):
        self.queue_index = int(job_queue_index)
        self.dag_file_path = dag_file_path
        self.dag = dag
        self.num_dag_nodes = num_dag_nodes
        self.requested_power = float(requested_power)
        self.requested_time = float(requested_time)
        self.deadline = float(requested_time)
        self.threshold = float(threshold)

    def record_resource(start_time, duration, power, num_nodes, workload):
        self.resources_consumed.append([float(start_time), float(duration),
            float(power), int(num_nodes), float(workload)])

    def update_job(job, job_runs, run_end_time, run_start_time, available_power, available_nodes):
        job.start_time = run_start_time
        job.completion_time = run_end_time
        job.available_power = available_power
        job.available_nodes = available_nodes

        ???????


    def setup_jobs(dag_names, policy, requested_powers = [], requested_times = [], thresholds = []):
        
        files = [f for f in os.listdir('policies')]
        policy_valid = False
        policies = []
        for f in files:
            name = f.split('.py')[0]
            if len(name) > 0:
                policies.append(name)
            if name == policy:
                policy_valid = True
        if not policy_valid:
            print("ERROR: Invalid policy specified. Please specify one of the following policies:")
            for name in policies:
                print("\t",name)
            sys.exit(0)
            
        applications = init_all_apps(num_apps)
        if policy == "POW-Opt":
            applications = reduce_application_space(applications)
            
        job_queue = []
        num_jobs = len(dag_names)
        
        for i in range(0, num_jobs):
            (task_graph, num_dag_nodes) = setup_dag(dag_names[i], applications, num_apps)
            requested_power = -1
            requested_time = -1
            threshold = -1
            if len(requested_powers) > 0:
                requested_power = requested_powers[i]
            if len(requested_times) > 0:
                requested_time = requested_times[i]
            if len(tresholds) > 0:
                threshold = thresholds[i]
            job = Job(i, dag_names[i], task_graph, num_dag_nodes, requested_power, requested_time, threshold)
            job_queue.append(job)
            
        return job_queue
        

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