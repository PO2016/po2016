from import_util import po2016

from po2016 import power_function_handler as pfun
from po2016.app_file_handler import load_app_file, load_task_file
from po2016.task import Task
from po2016.pace import get_pace
import copy
import sys


num_apps = 26

class Application(object):
    
    def __init__(self, index, idle = 0, pace = 0, race = 0):
        self.index = index
        self.idle = Task(0,0,0,0,0)
        self.pace = Task(0,0,0,0,0)
        self.race = Task(0,0,0,0,0)
        self.tasks = []
        
        self.power_function = []
        
    def get_power_function(ch):
        return 1
    
    def get_special_config_states(self, app_tasks):
        idle_found = False
        max_speed = 0
        fastest_task = None
        for task in app_tasks:
            if task.speed >= max_speed:
                max_speed = task.speed
                fastest_task = task
            if task.configIndex == -1:
                idle_found = True
                self.idle = task
        self.race = copy.copy(fastest_task)
        pace_task = get_pace(app_tasks)
        self.pace = copy.copy(pace_task)
        if idle_found == False:
            print("ERROR: Idle task not found")


appDir = "applications"

def init_all_apps(num_apps):
    applications = {}
    for i in range(0, num_apps):
        applications[i] = Application(i)
        tasks = load_task_file(i)
        applications[i].tasks = tasks
        applications[i].get_special_config_states(tasks)
        (pow_speed_pts, ps_configs) = pfun.get_pow_speed_points(tasks)
        applications[i] = pfun.process_convex_hull(pow_speed_pts, ps_configs, applications[i])
    return applications

def get_task_by_config(application, configuration):
    tsk = None
    for task in application.tasks:
        if task.speed == configuration[0] and task.power == configuration[1]:
            tsk = task
            break
    return tsk


def reduce_application_space(applications):
    new_app_dict = {}
    for index, application in applications.items():
        new_application = Application(index)
        new_application.power_function = application.power_function
        new_application.idle = application.idle
        new_application.pace = application.pace
        new_application.race = application.race
        new_task_list = []
        new_task_list.append(get_task_by_config(application, application.power_function[0].pt1))
        for segment in application.power_function:
            new_task_list.append(get_task_by_config(application, segment.pt2))
        new_application.tasks = new_task_list
        new_app_dict[index] = new_application
    return new_app_dict


def get_closest_task(taskFile, powerPerTask):
    minPowDiff = powerPerTask
    closestTask = None
    for task in taskFile:
        diff = powerPerTask
        if(task.power <= powerPerTask):
            diff = powerPerTask - task.power
            if(diff <= minPowDiff):
                minPowDiff = diff 
                closestTask = task
    return closestTask

def get_tasks_per_power(tasks, applications, power, power_cap):
    new_tasks = []
    waitlist = []
    for task in tasks:
        new_task = get_closest_task(applications[task.index].tasks, power)
        new_task.dag_index = task.dag_index
        if new_task.speed > 0:
            new_task.workload = task.workload
            new_task.time = new_task.workload / new_task.speed
        if new_task.configIndex == -1:
            waitlist.append(new_task)
            power = power_cap / (len(tasks) - len(waitlist))
            if power == power_cap and len(tasks) == 1:
                print("ERROR there is only one task and the power cap is not high enough.")
                sys.exit()
            if len(waitlist) == len(tasks):
                print("ERROR no tasks can be scheduled under the given power cap.")
                sys.exit()
            #print("ERROR Couldn't find task for the power allocated. Power = ", power)
            #sys.exit()
            #new_task = applications[task.index].idle
        else:
            new_tasks.append(new_task)
    z = len(new_tasks)
    new_tasks += waitlist
    return new_tasks, z
    