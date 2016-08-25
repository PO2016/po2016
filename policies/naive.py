from import_util import po2016
from po2016 import debug_log as dbg
from po2016.application import Application, init_all_apps, num_apps, get_tasks_per_power
from po2016.dag.dot2dag import setup_dag
from po2016.pace import get_pace_tasks
from po2016 import scheduler as sc
from po2016.scheduler import Schedule
from po2016.task import Task, print_task
from po2016.run import Run, print_run, get_total_runtime
from po2016 import job


DEBUG, TRACE = dbg.get_debug_level()


def process_job_queue_sequentially(job_queue, requested_nodes, system_nodes, power_cap, applications, outdir):

    num_jobs = len(job_queue)

    start_time = 0

    for i in range(0, num_jobs):
        job = job_queue[i]
        num_nodes = requested_nodes[i]
        if num_nodes > system_nodes:
            num_nodes = system_nodes
        alloc_power = power_cap / num_nodes

        output = naive(num_nodes, alloc_power, job, applications, start_time)

        if output == - 1:
            new_queue = job_queue[i+1:] + [job]
            job_queue = new_queue
        else:
            job.runs = output
            start_time += get_total_runtime(output)

    return job_queue

#this needs refinement
def schedule_jobs(job_queue, requested_nodes, system_nodes, power_cap, applications, outdir):


    num_jobs = len(job_queue)
    total_nodes_requested = sum(requested_nodes)
    
    if total_nodes_requested > system_nodes:
        total_nodes_requested = system_nodes
    
    pow_node = power_cap / total_nodes_requested

    start_time = 0

    available_nodes = system_nodes
    
    for i in range(0, num_jobs):
        
        job = job_queue[i]
        num_nodes = requested_nodes[i]
        
        if num_nodes <= available_nodes:
            
            available_nodes 
        
        alloc_power = pow_node * num_nodes
        output = naive(num_nodes, alloc_power, job, applications)
        if output == -1:
            new_queue = job_queue[i+1:] + [job]
            job_queue = new_queue
        else:
            job.runs = output
        
    return job_queue

def naive(num_machines, power_cap, job, applications, start_time = 0):
    """ The naive policy divides the available power equally among tasks, which
    are kept in a queue. If the required power of a task awaiting its turn voilates
    the power cap, it and all tasks depending on it remain in the task queue, while
    a less power needy task may proceed.
    
    Input parameters:
    num_machines - number of machine nodes requested
    power_cap - system-wide power cap
    job - current job to execute
    applications - dictionary with application index as key and Application object as value
    """
    
    task_graph = job.dag
    task_graph.num_dag_nodes = job.num_dag_nodes
    
    if TRACE:           
        print("===========")
        
        for el in task_graph:
            print("...")
            for e, val in el.items():
                print("\t", e, ": (", len(val), ")")
                print("\t\tapp_index: ", val[0].index, ", dag_index: ", val[0].dag_index)
        print("===========")    

    pace_tasks_toposorted = get_pace_tasks(task_graph)[0]
    
    runs = []
    #Number of completed tasks
    num_completed_tasks = 0

    #Index of next schedulable group 
    next_available = 0
    
    #List of all tasks that can be scheduled next (next in task_toposorted)
    ready_tasks = task_pace_toposorted[next_available]
    
    #Sort according to decreasing power
    ready_tasks = sorted(ready_tasks, key=lambda x: x.power, reverse = True)
    
    #Tasks currently running
    current_schedule = sc.Schedule()
    
    t = 0    
    
    while num_completed_tasks < num_dag_nodes:
        
        tasks_removed = []
        num_tasks_removed = 0
        
        remaining_power = power_cap - current_schedule.power_required
        
        if DEBUG:
            print("Remaining power:", remaining_power, "from power cap:", power_cap,
                  ", current power: ",current_schedule.power_required)
        
        z = min(len(ready_tasks), num_machines - len(current_schedule.tasks))
        
        if z > 0:
            power_per_run = remaining_power / z
        
            ready_tasks, z = get_tasks_per_power(ready_tasks, applications, power_per_run, power_cap)
        
        if z >= 1:
            if len(ready_tasks) > 0:
                if DEBUG:
                    print("Adding", z, "tasks to current schedule from group", next_available)
                ready_tasks = sc.add_tasks(ready_tasks, current_schedule, z)
                #Update remaining_power C'
                remaining_power = power_cap - current_schedule.power_required
                if DEBUG:
                    print("Remaining power:", remaining_power, "from power cap:", power_cap, ", current power: ",current_schedule.power_required)
            elif z == -1:
                return -1
            elif DEBUG:
                print("No more tasks to schedule until this group finishes processing")
                              
        sc.run_schedule(current_schedule, runs, num_machines)
        
        num_tasks_removed = len(current_schedule.tasks)
        num_completed_tasks += len(current_schedule.tasks)
        t = current_schedule.completion_time
        current_schedule.start_time = t
        current_schedule.tasks = []
        current_schedule.init_schedule()
        
        if DEBUG:
            print("Removed", num_tasks_removed, "tasks")
            print("Total number of completed tasks:", num_completed_tasks)
            
        if len(ready_tasks) == 0 and len(current_schedule.tasks) == 0:
            next_available += 1
            if next_available < len(task_pace_toposorted):
                ready_tasks = task_pace_toposorted[next_available]
                if DEBUG:
                    print("Ready for next group of available tasks")
            else:
                if DEBUG:
                    print("No more tasks to add.")
                if num_completed_tasks != num_dag_nodes:
                    print("ERROR, no more tasks left to process, but didn't reach initial number of tasks from the dag. Terminating..")
                    sys.exit(1)
                    break
        elif DEBUG:
            print("This group has not completed running yet")
                
    return runs