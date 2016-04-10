from task import Task
from app_file_handler import load_task_file

def get_pace_tasks(toposorted_tasks):
    all_pace_tasks = {}
    pace_sorted_dag = []

    for dictionary in toposorted_tasks:
        group = []
        for index, tasks in dictionary.items():
            app_id = tasks[0].index
            pace_task = get_pace(tasks)
            if app_id not in all_pace_tasks:
                all_pace_tasks[app_id] = pace_task
            group.append(pace_task)
        pace_sorted_dag.append(group)

    return pace_sorted_dag, all_pace_tasks


def get_pace(taskFile):
    highestTask = None
    maxVal = -1
    for task in taskFile:
        if task.power > 0:
            ratio = task.speed/task.power
        else:
            ratio = 0
        if(ratio >= maxVal):
            maxVal = ratio
            highestTask = task
    maxSpeed = 0
    #if there are multiple configs matching pace, pick the task with higher speed
    for task in taskFile:
        if task.power > 0:
            ratio = task.speed/task.power
        ratio = 0
        if ratio == maxVal:
            if task.speed >= maxSpeed:
                maxSpeed = task.speed
                highestTask = copy.copy(task)
    return highestTask