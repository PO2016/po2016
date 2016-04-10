import random
import copy
import networkx as nx

def dot2dag(dot):
    dag = {}
    for line in dot:
        if '{' not in line and '}' not in line:
            line = line.split('\n')[0]
            line = line.split("->")
            if len(line) == 2:
                parent = line[0]
                child = line[1].split()[0]
                parent = parent.strip('\n')
                parent = parent.replace(" ","")
                child = child.strip('\n')
                child = child.replace(" ", "")
                if parent not in dag:
                    dag[parent] = set()
                dag[parent].add(child)
            
    return dag

def order_dag(dag):
    G = nx.DiGraph()
    for key, val in dag.items():
        G.add_node(key)
        for v in val:
            G.add_edge(key, v)
    #print("Longest path: ", nx.dag_longest_path_length(G))        
    return nx.topological_sort(G)

def dag2indeces(dag, ordered):
    new_dag = []
    for node_set in dag:
        new_node_set = set()
        for node in node_set:
            new_node_set.add(ordered.index(node))
        new_dag.append(new_node_set)
    return new_dag

def apps2dag_sorted(sorted_dag):
    app_dag = []
    pos = 0
    app_id = 0
    for e in sorted_dag:
        app_dag.append(set())
        num_nodes = len(e)
        for n in range(num_nodes):
            app_id += 1
            app_dag[pos].add(app_id)
        pos += 1
    return app_dag
    
def dag2list(dag):
    l = []
    num_nodes = 0
    for dag_set in dag:
        list_set = []
        for el in dag_set:
            num_nodes += 1
            list_set.append(el)
        list_set.sort()
        l.append(list_set)
    l.sort(key=lambda x: x[0])
    return l, num_nodes

def dag2allTasks(dag, applications, num_apps):
    task_graph = []
    for group in dag:
        task_group_d = {}
        for index in group:
            app_id = random.randint(0, num_apps - 1)
            task_group_d[index] = copy.deepcopy(applications[app_id].tasks)
        task_graph.append(task_group_d)
    return task_graph

def add_dag_indeces(task_graph):
    wkld_base = 50
    for dictionary in task_graph:
        for dag_index, tasks in dictionary.items():
            workload = wkld_base + dag_index * random.random()
            for task in tasks:
                task.dag_index = dag_index
                task.workload = workload
    return task_graph