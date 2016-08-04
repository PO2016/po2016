class Run(object):
    
    def __init__(self, start_time = 0, end_time = 0, power_start = 0,
        power_end = 0, power = 0, speed = 0, workload = 0, app_id = -1,
        config_index = -1, dag_index = -1, node_index = -1):
        self.start_time = start_time
        self.end_time = end_time
        self.power_start = power_start
        self.power_end = power_end
        self.power = power
        self.speed = speed
        self.workload = workload
        self.app_id = app_id
        self.config_index = config_index
        self.dag_index = dag_index
        self.node_index = node_index
        
def get_total_runtime(runs):
    max_time = 0
    for run in runs:
        if run.end_time >= max_time:
            max_time = run.end_time
    return max_time

def get_runs_start_time(runs):
    min_time = 100000000000000000
    for run in runs:
        if run.start_time <= min_time:
            min_time = run.start_time
    return min_time

def get_runs_stats(runs):
    max_time = 0
    min_time = 100000000000000000
    max_power = 0
    workload = 0
    nodes = []

    for run in runs:
        if run.end_time >= max_time:
            max_time = run.end_time
        if run.start_time <= min_time:
            min_time = run.start_time
        if power_end >= max_power:
            max_power = power_end
        workload += run.workload
        if run.node_index not in nodes:
            nodes.append(run.node_index)

    return min_time, max_time, max_power, workload, len(nodes)

def print_run(run):
    if __debug__:
        print("======================")
        print("Executed run:")
        print("\tstart time:", run.start_time)
        print("\tend time:", run.end_time)
        print("\tpower start:", run.power_start)
        print("\tpower end:", run.power_end)
        print("\tpower:", run.power)
        print("\tspeed:", run.speed)
        print("\tworkload:", run.workload)
        print("\tapp index:", run.app_id)
        print("\tconfig index:", run.config_index)
        print("\tdag_index:", run.dag_index)
        print("\tnode_index:", run.node_index)
        print("======================\\end of print_run==")