import os
#from task import Task, print_task
#from run import Run, print_run
#import scheduler as sc
#from scheduler import Schedule
#from toposort import toposort, toposort_flatten
#from dag.dot2dag import dot2dag, order_dag, dag2list, dag2allTasks, add_dag_indeces, dag2indeces
#from pace import get_pace_tasks
#import power_function_handler as pfun
#from application import Application, init_all_apps, reduce_application_space, get_tasks_per_power
import sys, getopt

from import_util import po2016
from po2016 import job

#DEBUG = 0
#TRACE = 0
#NAIVE = 0
#POWOPT = 0

num_machines = 0
power_cap = 0
num_apps = 0

dag_name = ""

##############COMMAND LINE STUFF#####################

#myopts, args = getopt.getopt(sys.argv[1:],"k:C:a:d:N:P:D:T:")

#for o, a in myopts:
    #if o == '-k':
        #num_machines = int(a)
    #if o == '-C':
        #power_cap = float(a)
    #if o == '-a':
        #num_apps = int(a)
    #if o == '-d':
        #dag_name = a
    #if o == '-N':
        #NATIVE = 1
    #if o == '-P':
        #POWOPT = 1
    #if o == '-D':
        #DEBUG = 1
    #if o == '-T':
        #TRACE = 1

#if num_machines == 0 or power_cap == 0 or num_apps == 0 or len(dag_name) == 0 or (NAIVE == 0 and POWOPT == 0):
    #print("=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=")
    #print("Usage:")
    #print("\t-k [#number of machines]")
    #print("\t-C [power cap (W)]")
    #print("\t-a [#number of applications with power-performance measurements]")
    #print("\t-d [name of the dot file containing the DAG]")
    #print("\t-N [run the static/naive version]")
    #print("\t-P [run the Power-OPT algorithm]")
    #print("\t-D [debug mode]")
    #print("\t-T [trace mode]")
    #print("=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=")
    #sys.exit(0)

##################END OF COMMAND LINE STUFF######################


if DEBUG:
    print("Initializing all applications...")
applications = init_all_apps(num_apps)
if POWOPT:
    applications_po = reduce_application_space(applications)

if DEBUG:
    print("Getting dot dag...")

dot_dag_file = open(r"dag\\"+dag_name+".dot","r")
dot_dag = dot2dag(dot_dag_file)
dot_dag_file.close()

toposort_ordering = order_dag(dot_dag)

if DEBUG:
    print("This is the ordering of the DAG nodes:\n\t", toposort_ordering)

if DEBUG:
    print(dot_dag)
    print("Toposort on dot dag...")

dot_toposorted = list(toposort(dot_dag))

if DEBUG:
    print(dot_toposorted)

if DEBUG:
    print("Map app indeces on sorted dot dag...")
    
#The DAG of tasks already sorted, whose nodes are application indeces
task_graph_by_index = dag2indeces(dot_toposorted, toposort_ordering)
#task_graph_by_index = [{0,1,2},{3,4,5},{6,7,8}]
(task_graph_by_index, num_dag_nodes) = dag2list(task_graph_by_index)

if POWOPT:
    task_graph = dag2allTasks(task_graph_by_index, applications_po, num_apps)
else:
    task_graph = dag2allTasks(task_graph_by_index, applications, num_apps)

if POWOPT:
    task_graph_po = add_dag_indeces(task_graph)
if NAIVE:
    task_graph = add_dag_indeces(task_graph)
    
if NAIVE and POWOPT:
    if len(task_graph) != len(task_graph_po):
        print("ERROR, the index DAGs are of different sizes!")
        sys.exit(1)
        
if DEBUG:
    print(task_graph_by_index)
if TRACE:
    if NAIVE:
        
        print("===========")
        
        for el in task_graph:
            print("...")
            for e, val in el.items():
                print("\t", e, ": (", len(val), ")")
                print("\t\tapp_index: ", val[0].index, ", dag_index: ", val[0].dag_index)
        print("===========")
    if POWOPT:
        print("===========")
        
        for el in task_graph_po:
            print("...")
            for e, val in el.items():
                print("\t", e, ": (", len(val), ")")
                print("\t\tapp_index: ", val[0].index, ", dag_index: ", val[0].dag_index)
        print("===========")        

if DEBUG:
    print("Use indeces to get pace tasks...")

if POWOPT:
    (task_pace_toposorted_po, all_pace_tasks) = get_pace_tasks(task_graph_po)
if NAIVE:
    (task_pace_toposorted, all_pace_tasks) = get_pace_tasks(task_graph)

if DEBUG:
    print("Assigning power cap based on", len(all_pace_tasks), "tasks...")

if DEBUG:
    print("Power cap: " + str(power_cap) + " W")

print("Number of DAG nodes:", num_dag_nodes)

def naive(num_dag_nodes, task_pace_toposorted, task_graph, power_cap, applications):
    runs = []
    #Number of completed tasks
    num_completed_tasks = 0

    #Index of next schedulable group 
    next_available = 0
    
    #List of all tasks that can be scheduled next (next in task_toposorted)
    ready_tasks = task_pace_toposorted[next_available]
    
    #Tasks currently running
    current_schedule = sc.Schedule()
    
    t = 0
    
    while num_completed_tasks < num_dag_nodes:
        tasks_removed = []
        num_tasks_removed = 0
        remaining_power = power_cap - current_schedule.power_required
        if DEBUG:
            print("Remaining power:", remaining_power, "from power cap:", power_cap, ", current power: ",current_schedule.power_required)
        
        z = min(len(ready_tasks), num_machines - len(current_schedule.tasks))
        
        if z > 0:
            power_per_run = remaining_power / z
        
            ready_tasks = get_tasks_per_power(ready_tasks, applications, power_per_run)
        
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

def alg(num_dag_nodes, task_pace_toposorted, task_graph, power_cap, applications):
    
    #Number of completed tasks
    num_completed_tasks = 0

    #Index of next schedulable group 
    next_available = 0
    
    #List of all tasks that can be scheduled next (next in task_toposorted)
    ready_tasks = task_pace_toposorted[next_available]
    
    #Tasks currently running
    current_schedule = Schedule()
    
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


runs = []
runs_op = []

if POWOPT:
    runs_op = alg(num_dag_nodes, task_pace_toposorted_po, task_graph_po, power_cap, applications_po)
if NAIVE:
    runs = naive(num_dag_nodes, task_pace_toposorted, task_graph, power_cap, applications)

outDir = "output"

def runs2file(runs, fileName):
    outfile = open(outDir + "/" + fileName, "w")
    tab = "\t"
    nl = "\n"
    for run in runs:
        if run.config_index == -1:
            print("ERROR, found an idle task in run file", fileName, ", which is not supposed to happen.")
            outFile.close()
            sys.exit(1)
        line = str(run.start_time) + tab + str(run.end_time) + tab + str(run.power_start) + tab + str(run.power_end) + tab + str(run.power) + tab + str(run.speed) + tab + str(run.workload) + tab + str(run.app_id) + tab + str(run.config_index) + tab + str(run.dag_index) + nl
        outfile.write(line)
    outfile.close()

if POWOPT:    
    runs2file(runs_op, dag_name+".txt")
if NAIVE:
    runs2file(runs, "naive-"+dag_name+".txt")
        

###########################################################################



def verify_policy(policy):
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
    return policies

dag_names = ["..", "..", ".."]
nodes = [5,5,5]
power_cap = 10
system_nodes = 50

policy = 'naive'

outdir = ''

policies = verify_policy(policy)

if len(policies) > 0 and policy in policies:

    policy_module = __import__(po216.policies.policy)

    (job_queue, applications) = setup_jobs(dag_names, policy, outdir) #indir)

    job_queue = policy_module.schedule_jobs(job_queue, nodes, power_cap, applications, outdir)

    write_scheduler_output(job_queue, outdir)

else:
    print("ERROR: Something went wrong with executing the policy.")


def write_scheduler_output(job_queue, outdir):

