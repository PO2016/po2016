import os
import sys, getopt
import importlib
from import_util import po2016
from po2016 import job
from po2016 import debug_log as dbg

project_name = "po2016"

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
        

###########################################################################

dbg.set_debug_level(2)


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

dag_dirname = "dag"
dag_extension = ".dot"
dag1 = dag_dirname+"/"+"sample-dag2" +dag_extension
dag2 = dag_dirname+"/"+"/synth-sm-long"+dag_extension
dag3 = dag_dirname+"/"+"synth-sm-wide"+dag_extension

dag_names = [dag1, dag2, dag3]

nodes = [10, 15, 20]

power_cap = 6000

system_nodes = 50

policy = 'naive'

outdir = 'testing_output'

policies_list = verify_policy(policy)

if len(policies_list) > 0 and policy in policies_list:
    
    module_name = project_name + ".policies." + policy
    
    policy_module = importlib.import_module(module_name)

    (job_queue, applications) = job.setup_jobs(dag_names, policy, app_outdir = outdir) #indir)

    job_queue = policy_module.schedule_jobs(job_queue, system_nodes, nodes, power_cap, applications, outdir)

    write_scheduler_output(job_queue, outdir)

else:
    print("ERROR: Something went wrong with executing the policy.")


def write_scheduler_output(job_queue, outdir):
    return 0

 
    