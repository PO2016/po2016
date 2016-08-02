from import_util import po2016
from po2016 import debug_log as dbg
from po2016.application import Application, init_all_apps, num_apps, get_tasks_per_power
from po2016.dag.dot2dag import setup_dag
from po2016.pace import get_pace_tasks
from po2016 import scheduler as sc
from po2016.run import Run

DEBUG, TRACE = dbg.get_debug_level()
    
def POW_Opt(num_machines, power_cap, job, applications):
    
    task_graph = job.dag
    num_dag_nodes = job.num_dag_nodes
    
    (task_pace_toposorted, all_pace_tasks) = get_pace_tasks(task_graph)
    
    #Number of completed tasks
    num_completed_tasks = 0

    #Index of next schedulable group 
    next_available = 0
    
    #List of all tasks that can be scheduled next (next in task_toposorted)
    ready_tasks = task_pace_toposorted[next_available]
    
    #Tasks currently running
    current_schedule = sc.Schedule()
    
    #List of all the 'rectangles' to draw
    runs = []
    
    #Current time checked
    t = 0
    
    while num_completed_tasks < num_dag_nodes:
        tasks_removed = []
        num_tasks_removed = 0
        
        #Sort according to decreasing power
        ready_tasks = sorted(ready_tasks, key=lambda x: x.power, reverse = True)
        
        remaining_power = power_cap - current_schedule.power_required
        if DEBUG:
            print("Remaining power:", remaining_power, "from power cap:", power_cap, ", current power: ",current_schedule.power_required)
        
        y = 0
        
        #update y if p_pace_j <= C'
        if len(ready_tasks) > 0:
            if ready_tasks[0].power <= remaining_power:
                sum_power = 0
                for task in ready_tasks:
                    if sum_power + task.power <= remaining_power:
                        y += 1
                    else:
                        break
                    sum_power += task.power
        
        num_free_machines = abs(num_machines - len(current_schedule.tasks))
        z = min(y, num_free_machines)
        if DEBUG:
            print("z", z, "y", y, "free_machines", num_free_machines)
            
        if z >= 1:
            if len(ready_tasks) > 0:
                if DEBUG:
                    print("Adding", z, "tasks to current schedule from group", next_available)
                ready_tasks = sc.add_tasks(ready_tasks, current_schedule, z)
                #Update remaining_power C'
                remaining_power = power_cap - current_schedule.power_required
                if DEBUG:
                    print("Remaining power:", remaining_power, "from power cap:", power_cap, ", current power: ",current_schedule.power_required)
            elif DEBUG:
                print("No more tasks to schedule until this group finishes processing")
                
        if DEBUG:        
            print("Current schedule now contains", len(current_schedule.tasks), "tasks")
        
        if current_schedule.power_required < power_cap / 2 and len(current_schedule.tasks) > 0:
            if DEBUG:
                print("Consuming less than half the power budget")
            (current_schedule, runs, tasks_removed, num_tasks_removed) = sc.high_low(current_schedule, t, runs, num_machines - num_free_machines, power_cap, applications)
            
        else:
            (runs, t, tasks_removed) = sc.finish_min_task(current_schedule, runs)
            (runs, num_tasks_removed) = sc.process_schedule(current_schedule, t, runs, tasks_removed)
        
        num_completed_tasks += num_tasks_removed
        
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