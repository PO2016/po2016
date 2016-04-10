class Task(object):
    
    def __init__(self, index, configIndex, power, time, speed, workload = 0, dag_index = -1):
        self.index = int(index)
        self.dag_index = int(index)
        self.configIndex = int(configIndex)
        self.time = float(time)
        self.speed = float(speed)
        self.power = float(power)
        self.workload = float(workload)
        self.original_workload = float(workload)
        
        
    def allocate_power(self, power):
        self.allocatedPower = float(power)
        
    def __eq__(self, other): 
        return self.__dict__ == other.__dict__ 
    

def print_task(task):
    if __debug__:
        print("+++++++++++++++++++++")
        print("Task:")
        print("\tindex:",task.index)
        print("\tdag_index", task.dag_index)
        print("\tconfig index", task.configIndex)
        print("\ttime", task.time)
        print("\tpower", task.power)
        print("\tspeed", task.speed)
        print("\tworkload", task.workload)
        print("+++++++++++++++++++++")