from job import Job, update_job



def find_best_time(job, available_power, available_nodes, current_time):
    ????



def RMAP_adaptive(job_queue, power_cap, total_num_nodes):

    available_power = power_cap
    available_nodes = total_num_nodes
    runs = []

    t = 0

    for job in job_queue:

        setup_job_dag(job)

        if job.requested_power <= available_power:
            (job_runs, best_time) = find_best_time(job, job.requested_power,
                available_nodes, t)
            runs += job_runs

        else:
            (job_runs, best_time) = find_best_time(job, available_power,
                available_nodes, t)

            if best_time > (1 + job.threshold) * job.requested_time:
                new_queue = job_queue[1:]
                new_queue.append(job)
                job_queue = new_queue
            
            else:
                runs += job_runs

        update_job(job, job_runs, best_time, t, available_power, available_nodes)


        if job.max_nodes < available_nodes:
            available_nodes -= job.max_nodes
            available_power = power_cap - job.completion_power
        else:
            t = best_time
            available_nodes = total_num_nodes
            available_power = power_cap