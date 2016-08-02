from import_util import po2016
from po2016 import power_function_handler as pfun
from po2016 import application
from po2016.run import Run, print_run
import copy
from po2016.task import print_task
import sys

import numpy as np

DEBUG = 0
TRACE = 0

class Schedule(object):
    
    def __init__(self):
        
        self.tasks = []
        self.completion_time = 0
        self.power_required = 0
        self.max_power = 0
        self.max_power_task = None
        self.min_power = 0
        self.min_power_task = None
        self.max_speed = 0
        self.fastest_task = None
        self.min_speed = 0
        self.slowest_task = None
        self.max_time = 0
        self.longest_task = None
        self.min_time = 0
        self.fastest_task = None
        self.start_time = 0
        
    def add_task(self, task):
        self.tasks.append(task)
        if TRACE:
            print("**************************")
            print("Added task to schedule:")
            print_task(task)
            print("**************************")
        
    def remove_task(self, task):
        for sctask in self.tasks:
            if sctask.index == task.index:
                self.tasks.remove(sctask)
                
    def get_max(self, attr):
        max_attr = -1
        max_task = None
        for sctask in self.tasks:
            scattr = getattr(sctask, attr)
            if scattr >= max_attr:
                max_attr = scattr
                max_task = sctask
        return max_attr, max_task              
    
    def get_min(self, attr):
        min_attr = 10000000000000000
        min_task = None
        for sctask in self.tasks:
            scattr = getattr(sctask, attr)
            if scattr <= min_attr:
                min_attr = scattr
                min_task = sctask
        return min_attr, min_task      

    def get_max_time(self):
        (max_time, max_task) = self.get_max('time')
        self.completion_time = max_time + self.start_time
        self.max_time = max_time
        self.longest_task = max_task
        
    def get_max_power(self):
        (self.max_power, self.max_power_task) = self.get_max('power')
        
    def get_max_speed(self):
        (self.max_speed, self.fastest_task) = self.get_max('speed')
        
    def get_min_time(self):
        (self.min_time, self.shortest_task) = self.get_min('time')
        
    def get_min_power(self):
        (self.min_power, self.min_power_task) = self.get_min('power')
        
    def get_min_speed(self):
        (self.min_speed, self.slowest_task) = self.get_min('speed')
        
    def get_power_required(self):
        total_power = 0
        for sctask in self.tasks:
            total_power += sctask.power
        self.power_required = total_power
        
    def init_schedule(self):
        self.get_max_time()
        self.get_min_time()
        self.get_max_power()
        self.get_min_power()
        self.get_max_speed()
        self.get_min_speed()
        self.get_power_required()

def print_schedule(sc):
    if __debug__:
        print(".....................")
        print("Schedule:")
        print("\tnum_tasks: ", len(sc.tasks))
        print("\tcompletion_time:",sc.completion_time)
        print("\tpower required:", sc.power_required)
        print("\tmax power:",sc.max_power)
        print("\tmin power:", sc.min_power)
        print("\tmax speed:", sc.max_speed)
        print("\tmin speed:", sc.min_speed)
        print("\tmax time:", sc.max_time)
        print("\tmin time:", sc.min_time)
        print("\tstart_time: ", sc.start_time)
        if len(sc.tasks) > 0:
            print("\tmax power task dag index:",sc.max_power_task.dag_index)
            print("\tmin power task dag index:", sc.min_power_task.dag_index)    
            print("\tfastest task dag index:", sc.fastest_task.dag_index)    
            print("\tslowest task dag index:", sc.slowest_task.dag_index)    
            print("\tlongest task dag index:", sc.longest_task.dag_index)    
            print("\tfastest task dag index:", sc.fastest_task.dag_index)        
        print(".....................")

def dag_index_added(schedule, task_to_add):
    found = False
    for task in schedule.tasks:
        if task.dag_index == task_to_add.dag_index:
            found = True
            break
    return found
       
def add_tasks(tasks, sc, num_tasks):
    changed = False
    rem_tasks = []
    if len(tasks) > 0:
        for i in range(0, num_tasks):
            if TRACE:
                print(i)
            if i < len(tasks):
                sc.add_task(copy.deepcopy(tasks[i]))
                changed = True
        if changed == True:    
            sc.init_schedule()
        if num_tasks < len(tasks):
            for i in range(num_tasks, len(tasks)):
                rem_tasks.append(copy.deepcopy(tasks[i]))
    if DEBUG:
        print("End of add_tasks(): schedule length:", len(sc.tasks))
    return rem_tasks


def finish_min_task(sc, runs):
    if DEBUG:
        print("beginning of finish min task: Num tasks in schedule:", len(sc.tasks))
    min_time = 0
    current_power = 0
    tasks_to_remove = []
    num_tasks_removed = 0
    for task in sc.tasks:
        if task.time == sc.min_time:
            min_time = sc.min_time
            if task.index == -1:
                print("ERROR, trying to schedule an idle task")
                sys.exit(1)
            run = Run(sc.start_time, sc.start_time + task.time, current_power, current_power + task.power, task.power, task.speed, task.workload, task.index, task.configIndex, task.dag_index)
            runs.append(run)
            if TRACE:
                print_run(run)
            if DEBUG:
                print("Finishing fastest task from schedule.")
            tasks_to_remove.append(task)
        current_power += task.power
    if DEBUG:    
        print("end of finish min task: Num tasks in schedule:", len(sc.tasks))
    return runs, min_time + sc.start_time, tasks_to_remove 

def process_schedule(current_schedule, t, runs, tasks_to_remove):
    if DEBUG:
        print("beginning of process schedule: Num tasks in schedule:", len(current_schedule.tasks))
    current_power = 0
    num_tasks_removed = len(current_schedule.tasks)
    if len(current_schedule.tasks) > 0:   
        for task in current_schedule.tasks:
            if task not in tasks_to_remove:
                workload_processed = task.speed * (t - current_schedule.start_time)
                task.workload = task.workload - workload_processed
                task.time = task.workload / task.speed
                run = Run(current_schedule.start_time, t, current_power, current_power + task.power, task.power, task.speed, workload_processed, task.index, task.configIndex, task.dag_index)
                runs.append(run)
                if DEBUG:
                    print("Processing schedule, run completed at time t=", t)
                if TRACE:
                    print_run(run)
            current_power += task.power
    
    for task in tasks_to_remove:
        for sc_task in current_schedule.tasks:
            if task.dag_index == sc_task.dag_index:
                current_schedule.remove_task(sc_task)
    if DEBUG:
        print("process schedule: Num tasks in schedule:", len(current_schedule.tasks))
    current_schedule.init_schedule()
    current_schedule.start_time = t
    if DEBUG:
        print("end of process schedule: Num tasks in schedule:", len(current_schedule.tasks))
    return runs, num_tasks_removed - len(current_schedule.tasks)

def handle_race_case(schedule, runs, power_cap, num_machines, t, applications):
    race_power_sum = 0
    race_tasks = []
    terminate = False
    runnable_sc = copy.copy(schedule)
    runnable_sc.tasks = []
    for task in schedule.tasks:
        race_task = copy.copy(applications[task.index].race)
        race_task.dag_index = task.dag_index
        #update workload
        race_task.workload = race_task.workload - (task.original_workload - task.workload)
        #update time
        race_task.time = race_task.workload / race_task.speed
        race_power_sum += race_task.power
        race_tasks.append(race_task)
    if DEBUG:
        print("Checking if needing to process", len(race_tasks), "race tasks")
    if race_power_sum <= power_cap:
        if DEBUG:
            print("Run current schedule at all race.")
        terminate = True
        for task in race_tasks:
            runnable_sc.tasks.append(task)
            if DEBUG:
                print("This race task is runnable:")
            if TRACE:
                print_task(task)
        runnable_sc.init_schedule()
        runs = run_schedule(runnable_sc, runs, num_machines)
        schedule.start_time += runnable_sc.max_time
        
    return terminate, runs

def get_min_delta(tasks, applications):
    min_delta = 1000000000000
    for task in tasks:
        race_task = applications[task.index].race
        delta = race_task.speed / task.workload
        if delta <= min_delta:
            min_delta = delta  
    return min_delta
        
def get_delta_tasks(tasks, applications, min_delta):
    delta_tasks = []
    for task in tasks:
        race_task = applications[task.index].race
        delta = race_task.speed / task.workload
        if delta == min_delta:
            race_task.workload = task.workload
            race_task.time = task.workload / race_task.speed
            race_task.dag_index = task.dag_index
            delta_tasks.append(race_task)
    return delta_tasks

def get_min_delta_pow_sum(tasks, applications, min_delta):
    pow_sum = 0
    for task in tasks:
        speed = task.workload * min_delta
        power = pfun.get_power_from_pow_fun(applications[task.index], speed)
        pow_sum += power
    return pow_sum


def find_satisfying_delta(tasks, applications, power, speeds):
    thresh = 25
    pow_sum = 0
    upper_pow = []
    lower_pow = []
    half = []
    pos = 0
    
    tasks = sorted(tasks, key=lambda x: x.power, reverse = True)
    
    for task in tasks:
        upper_pow.append(applications[task.index].race.power)
        lower_pow.append(applications[task.index].idle.power)
        half.append(lower_pow[pos] + ((upper_pow[pos] - lower_pow[pos]) / 2))
        pos += 1
    pow_sum = sum(half)
    init_upper = upper_pow
    init_lower = lower_pow
    while pow_sum != power:
        if pow_sum > power:
            upper_pow = half
            half = []
        else:
            lower_pow = half
            half = []
        for i in range(0, len(tasks)):
            half.append(lower_pow[i] + ((upper_pow[i] - lower_pow[i]) / 2))
        pow_sum = sum(half)
        diff = abs(power - pow_sum)
        if diff <= thresh and pow_sum != power:
            if pow_sum > power:
                for i in range(0, len(half)):
                    if (half[i] - init_lower[i]) >= diff:
                        half[i] -= diff
                        break
            else:
                last = len(half) - 1
                for i in reversed(range(last + 1)):
                    if (init_upper[i] - half[i]) >= diff:
                        half[i] += diff
                        break
                    
            pow_sum = sum(half)
            
    for i in range(0, len(tasks)):
        speed = pfun.get_speed_from_pow_fun(applications[tasks[i].index], half[i])
        index_speed = {}
        index_speed[tasks[i].index] = speed
        speeds[tasks[i].dag_index] = index_speed
    return speeds

def find_avg(current_schedule, num_machines, power_cap, applications):
    if DEBUG:
        print("FIND-AVG procedure called")
    tasks_remaining = list(current_schedule.tasks)
    tasks_processed = 0
    remaining_power = power_cap
    speeds = {}
    while tasks_processed < len(current_schedule.tasks):
        min_delta = get_min_delta(tasks_remaining, applications)
        delta_tasks = get_delta_tasks(tasks_remaining, applications, min_delta)
        pow_sum = get_min_delta_pow_sum(tasks_remaining, applications, min_delta)
        
        if pow_sum < remaining_power:
            race_pow = 0
            for task in delta_tasks:
                index_speed = {}
                index_speed[task.index] = task.speed
                speeds[task.dag_index] = index_speed
                race_pow += task.power
            remaining_power = remaining_power - race_pow
            for task in tasks_remaining:
                for delta_task in delta_tasks:
                    if task.dag_index == delta_task.dag_index:
                        tasks_remaining.remove(task)
                        tasks_processed += 1
        elif pow_sum >= remaining_power:
            speeds = find_satisfying_delta(tasks_remaining, applications, remaining_power, speeds)
            tasks_processed += len(tasks_remaining)
            tasks_remaining = []
            tasks_processed = len(current_schedule.tasks)
    return speeds

def get_high_low_configs(speeds, applications, num_machines):
    high_low = {}
    for dag_index, index_speed in speeds.items():
        for index, speed in index_speed.items():
            hl_config = []
            for segment in applications[index].power_function:
                s1 = segment.pt1[0]
                s2 = segment.pt2[0]
                if speed >= s1 and speed <= s2:
                    hl_config.append(segment.pt1)
                    hl_config.append(segment.pt2)
                    break
            high_low[dag_index] = [speed, hl_config]
    return high_low
        
        
def run_schedule(schedule, runs, num_machines):
    current_power = 0
    for task in schedule.tasks:
        if task.configIndex == -1:
            print("ERROR, found an idle task in schedule.")
            sys.exit(1)
        run = Run(schedule.start_time, schedule.start_time + task.time, current_power, current_power + task.power, task.power, task.speed, task.workload, task.index, task.configIndex, task.dag_index)
        runs.append(run)
        if TRACE:
            print_run(run)
        current_power += task.power
    return runs


def get_power_up_to_task(schedule, task):
    current_power = 0
    for t in schedule.tasks:
        if t == task:
            break
        current_power += t.power
    return current_power

def process_high_schedule(schedule, runs, power_cap, num_machines):
    if schedule.power_required <= power_cap:
        return run_schedule(schedule, runs, num_machines)
    sc = copy.copy(schedule)
    sc.tasks = []
    current_power = 0
    rem_tasks = []
    for task in schedule.tasks:
        if current_power + task.power <= power_cap:
            sc.tasks.append(copy.copy(task))
        else:
            rem_tasks.append(copy.copy(task))
        current_power += task.power
    sc.init_schedule()
    processed_tasks = 0
    rem = len(schedule.tasks)
    while processed_tasks < rem:
        (runs, t, tasks_removed) = finish_min_task(sc, runs)
        (runs, num_tasks_removed) = process_schedule(sc, t, runs, tasks_removed)
        processed_tasks += num_tasks_removed
        free_power = power_cap - sc.power_required
        cumulative_power = 0
        for task in rem_tasks:
            if cumulative_power + task.power <= free_power:
                cumulative_power += task.power
                sc.add_task(task)
                rem_tasks.remove(task)

        sc.init_schedule()
                
    return runs
    
       
def high_low(current_schedule, t, runs, num_machines, power_cap, applications):
    init_num_tasks = len(current_schedule.tasks)
    init_tasks = list(current_schedule.tasks)
    sc = copy.copy(current_schedule)
        
    terminate = False    
    (terminate, runs) = handle_race_case(current_schedule, runs, power_cap, num_machines, t, applications)
    if terminate == True:
        current_schedule.tasks = []
        current_schedule.init_schedule()
        if TRACE:
            print_schedule(current_schedule)
        return current_schedule, runs, init_tasks, init_num_tasks
    print("Finding average...")
    speeds = find_avg(sc, num_machines, power_cap, applications)
    print("Finished Finding average.")
    highLow = get_high_low_configs(speeds, applications, num_machines)
    if TRACE:
        print("high-low configurations:")
        print(highLow)
    
    low_schedule = Schedule()
    low_schedule.start_time = sc.start_time
    high_schedule = Schedule()
    
    max_low = 0
    for task in sc.tasks:
        speed_star = highLow[task.dag_index][0]
        hl_config = highLow[task.dag_index][1]
        low_speed = hl_config[0][0]
        high_speed = hl_config[1][0]
        t_low = 0
        t_high = 0
        
        if low_speed == high_speed or speed_star == low_speed:
            t_low = task.workload / low_speed
        elif low_speed == high_speed or speed_star == high_speed:
            t_low = task.workload / high_speed
        else:
            t_low = (task.workload * (1 - (high_speed/speed_star))) / (low_speed - high_speed)
            t_high = task.workload * ((low_speed - speed_star) / (speed_star * (low_speed - high_speed)))
        
        if t_low < t or t_high < t:
            print("ERROR: incorrect t_low or t_high computed:", t_low, t_high, t)
            sys.exit(1)
            
        if t_low >= max_low:
            max_low = t_low
            
        
        low_task = application.get_task_by_config(applications[task.index], hl_config[0])
        low_task.time = t_low
        if t_high > 0:
            high_task = application.get_task_by_config(applications[task.index], hl_config[1])
            high_task.time = t_high
            high_task.workload = t_high * high_task.speed
            high_task.dag_index = task.dag_index
            high_schedule.tasks.append(high_task)
        if low_task.speed != low_speed:
            print("Something went wrong with retrieving low config")
            sys.exit(1)
        low_task.workload = t_low * low_task.speed
        low_task.dag_index = task.dag_index
        low_schedule.tasks.append(low_task)
       
    low_schedule.init_schedule()
    
    runs = run_schedule(low_schedule, runs, num_machines)
    
    if len(high_schedule.tasks) > 0:
        high_schedule.start_time = max_low
        high_schedule.init_schedule()
        high_tasks = high_schedule.tasks
        high_tasks = sorted(high_tasks, key=lambda x: x.power, reverse = True)
        high_schedule.tasks = high_tasks
        print("Processing high_schedule..")
        runs = process_high_schedule(high_schedule, runs, power_cap, num_machines)
        print("Finished processing high_schedule") 
    sc.tasks = []
    
    return sc, runs, init_tasks, init_num_tasks