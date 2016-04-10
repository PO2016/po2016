class Run(object):
    
    def __init__(self, start_time = 0, end_time = 0, power_start = 0, power_end = 0, power = 0, speed = 0, workload = 0, app_id = -1, config_index = -1, dag_index = -1):
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
        print("======================\\end of print_run==")