from scipy.spatial import ConvexHull
from app_file_handler import load_task_file

import matplotlib.pyplot as plt

class Segment(object):
    
    def __init__(self):
        self.pt1 = [0,0]
        self.pt2 = [0,0]
        self.c1 = 0
        self.c2 = 0


def get_pow_speed_points(app_tasks):
    pow_speed_pts = []
    ps_configs = {}
    pos = 0
    for at in app_tasks:
        pt = []
        pt.append(at.speed)
        pt.append(at.power)
        pow_speed_pts.append(pt)
        ps_configs[pos] = (at.configIndex, pt)
        pos += 1
    return pow_speed_pts, ps_configs
    
    
    
def process_convex_hull(points, ps_configs, app):
    ch = ConvexHull(points)
    vertices = ch.vertices
    idle_coord = []
    idle_coord.append(app.idle.speed)
    idle_coord.append(app.idle.power)
    
    race_coord = []
    race_coord.append(app.race.speed)
    race_coord.append(app.race.power)
    
    idle_vert = points.index(idle_coord)
    race_vert = points.index(race_coord)
    
    pow_fun_verts = []
    idle_found = False
    race_found = False
    temp = []
    #The vertex list from convex hull will always be in counterclockwise order
    while idle_found == False and race_found == False:
        if idle_found == False:
            for vertex in vertices:
                if vertex == idle_vert:
                    idle_found = True
                if idle_found == True and vertex == race_vert:
                    race_found = True
                    temp.append(vertex)
                    break
                if idle_found == True and race_found == False:
                    temp.append(vertex)
        if idle_found == True and race_found == False:
            for vertex in vertices:
                if vertex == race_vert:
                    race_found = True
                if race_found == False:
                    temp.append(vertex)
            if race_found == True:
                temp.append(race_vert)
 
    for i in range(0, len(temp)-1):
        segment = Segment()
        segment.pt1[0] = points[temp[i]][0]
        segment.pt1[1] = points[temp[i]][1]
        segment.pt2[0] = points[temp[i+1]][0]
        segment.pt2[1] = points[temp[i+1]][1]
        
        if ps_configs[temp[i]][1] == points[temp[i]]:
            segment.c1 = ps_configs[temp[i]][0]
        else:
            for el in ps_configs:
                if el[1] == points[temp[i]]:
                    spegment.c1 = el[0]
        if ps_configs[temp[i+1]][1] == points[temp[i+1]]:
            segment.c2 = ps_configs[temp[i+1]][0]
        else:
            for el in ps_configs:
                if el[1] == points[temp[j+1]]:
                    spegment.c2= el[0]            
        
        pow_fun_verts.append(segment)
        
    app.power_function = pow_fun_verts

    return app


def get_power_from_pow_fun(application, speed):
    power = 0
    for segment in application.power_function:
        s1 = segment.pt1[0]
        s2 = segment.pt2[0]
        if speed >= s1 and speed <= s2:
            
            power = ((segment.pt2[1] - segment.pt1[1]) / (segment.pt2[0] - segment.pt1[0])) * (speed - segment.pt1[0]) + segment.pt1[1]
            
            break
        
    return power

def get_speed_from_pow_fun(application, power):
    speed = 0
    for segment in application.power_function:
        p1 = segment.pt1[1]
        p2 = segment.pt2[1]
        if power >= p1 and power <= p2:

            speed = (power - segment.pt1[1] + ((segment.pt2[1] - segment.pt1[1])/(segment.pt2[0] - segment.pt1[0])) * segment.pt1[0]) / ((segment.pt2[1] - segment.pt1[1])/(segment.pt2[0] - segment.pt1[0]))

            break
        
    return speed 