DEBUG = 0
TRACE = 0 

def set_debug_level(level):
    global DEBUG
    global TRACE
    if level == 1 or level == "debug" or level == "Debug" or level == "DEBUG" or level == "d" or level == "D":
        DEBUG = 1
        TRACE = 0
        print("DEBUG switched on.")
    elif level == 2:
        DEBUG = 1
        TRACE = 1
        print("DEBUG and TRACE switched on.")
    elif level == "trace" or level == "Trace" or level == "TRACE" or level == "t" or level == "T":
        DEBUG = 0
        TRACE = 1
        print("TRACE switched on.")
    else:
        DEBUG = 0
        TRACE = 0
    #return DEBUG, TRACE    