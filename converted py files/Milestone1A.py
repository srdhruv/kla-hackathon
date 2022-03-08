#!/usr/bin/env python
# coding: utf-8

# In[1]:


#56_Dhruv_Rajendra_Sawarkar_IITDM
#Importing necessary packages
import yaml
from datetime import datetime
import time
import threading
import os


# In[2]:


yaml_data = {}
example_fileName = "Milestone1/Milestone1_Example.yaml"
fileA = "Milestone1/Milestone1A.yaml"
fileB = "Milestone1/Milestone1B.yaml"
logFileName = "Milestone1/MileStone1A_Log.txt"
example = 0
filename = fileA
if example == 1:
    filename = example_fileName
with open(filename) as file:
    yaml_data = yaml.safe_load(file)


# In[4]:


all_data = {}
global root_logs
root_logs = ""
global history_done
history_done = {}


# In[5]:


#creating log file
with open(logFileName,"w") as file:
    file.write("")
def writeLogInFile(log):
    with open(logFileName,"a") as file:
        file.write(log)


# In[6]:


def addLog(name,status,optional=""):
    #date time;name status optional
    log = ""
    now = datetime.now() #2022-03-07 10:00:07.000000;
    neededFormat = now.strftime("%Y-%m-%d %H:%M:%S.%f;")
    log += neededFormat
    log += name
    log += status
    log += optional
    log += "\n"
    writeLogInFile(log)


# In[7]:


def doTask(name,task):
    if task['Function'] == "TimeFunction":
        extra = "TimeFunction ("
        values_list = list(task['Inputs'].values())
        n = len(values_list)
        for i in range(n):
            extra += values_list[i]
            if i != n-1:
                extra += ","
        extra += ')'
        addLog(name," Executing ",extra)
        time.sleep(int(task['Inputs']['ExecutionTime']))
        
        


# In[8]:


def doTaskSerial(name,task):
    if task['Type'] == 'Flow':
        addLog(name," Entry")
        #do it
        if task['Execution'] == 'Sequential':
            for subtaskName,subtask in task['Activities'].items():
                doTaskSerial(name+"."+subtaskName,subtask)
        elif task['Execution'] == 'Concurrent':
            # do parallel task
            subtasks = []
            threadList = []
            for subtaskName,subtask in task['Activities'].items():
                t = threading.Thread(target = doTaskSerial,args=(name+"."+subtaskName,subtask))
                threadList.append(t)
            for t in threadList:
                t.start()
            for t in threadList:
                t.join()
        else:
            print("\n\n______not serial nor paralle __________\n\n")
        addLog(name," Exit")
    elif task['Type'] == 'Task':
        # no need to go down just do it
        addLog(name," Entry")
        history_done[name] = "notDone"
        doTask(name,task)
        addLog(name," Exit")
        history_done[name] = "done"
            
            
        


# In[9]:


for k,v in yaml_data.items():
    doTaskSerial(k,v)


# In[10]:


with open(logFileName) as file:
    for line in file:
        print(line)

