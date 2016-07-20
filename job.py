from dag import print_dag_info
from run import Run

class Job(object):

    def __init__(self, job_queue_index, dag_file_path, dag, requested_power, requested_time,
        threshold, start_time = 0.0, available_power = 0.0, available_nodes = 0.0, completion_power = 0.0, completion_time = 0.0,
        completion_workload = 0.0, max_nodes = 0, resources_consumed = []):
        self.queue_index = int(job_queue_index)
        self.dag_file_path = dag_file_path
        self.dag = dag
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

    def print_job(job, debug_mode):
        print("++++++++++++JOB INFO+++++++++++++++")
        print("Job:")
        print("\tqueue_index:", job.queue_index)
        print("\tdag file name:", job.dag_file_path)
        if debug_mode == TRACE:
            print("\tdag:", print_dag_info(job.dag))
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