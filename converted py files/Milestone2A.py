#!/usr/bin/env python
# coding: utf-8

# In[1]:


#56_Dhruv_Rajendra_Sawarkar_IITDM
import yaml
from datetime import datetime
import time
import threading
import os
import pandas as pd


# In[2]:


yaml_data = {}
example_fileName = "Milestone2/Milestone1_Example.yaml"
fileA = "Milestone2/Milestone2A.yaml"
fileB = "Milestone2/Milestone2B.yaml"
logFileName = "Milestone2/MileStone2A_Log.txt"
example = 0
filename = fileA
dataset_folder = "Milestone2/"

if example == 1:
    filename = example_fileName
with open(filename) as file:
    yaml_data = yaml.safe_load(file)


# In[3]:


# yaml_data


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


def readCsv(filename):
    loc = dataset_folder + filename
    df = pd.read_csv(loc)
    noOfDefects = len(df)
    return df,noOfDefects


# In[8]:


def checkInputExists(vname):
    if vname[0] != '$':
        return 1
    vname = vname[2:-1]
    print("searching key ",vname)
    if vname in all_data:
        return 1
    while vname not in all_data:
        ("")
#         time.sleep(1)
        
    return 1

def waitForInputs(task):
    for k,inp in task['Inputs'].items():
        checkInputExists(inp)
    
def checkCondition(cond):
    string = cond[2:]
    lst = string.split(' ')
    variable = lst[0][:-1]
    operator = lst[1]
    value = int(lst[2])
#     print(variable,operator,value)
    if variable not in all_data:
        return -1
    if operator == '>' and all_data[variable] > value:
        return 1
    elif operator == '<' and all_data[variable] < value:
        return 1
    elif operator == '>=' and all_data[variable] >= value:
        return 1
    elif operator == '<=' and all_data[variable] <= value:
        return 1
    elif operator == '==' and all_data[variabe] == value:
        return 1
    else :
        return 0
            
    
    


# In[10]:


def doTask(name,task):
    if task['Function'] == "TimeFunction":
        
        
        if 'Condition' in task:
            returnVal = checkCondition(task['Condition'])
            if  returnVal == 0:
                addLog(name," Skipped")
                return
            elif returnVal == -1:
                print("\n\nKey does not exist Yet!!!")
                while(1):
                    returnVal = checkCondition(task['Condition'])
                    if returnVal != -1:
                        break
        waitForInputs(task)
        
        extra = "TimeFunction ("
        values_list = list(task['Inputs'].values())
        n = len(values_list)
        for i in range(n):
            if values_list[i][0] == '$':
                extra += str(all_data[values_list[i][2:-1]])
            else:
                extra += values_list[i]
            if i != n-1:
                extra += ","
        extra += ')'
        addLog(name," Executing ",extra)
        time.sleep(int(task['Inputs']['ExecutionTime']))
        
        
    elif task['Function'] == 'DataLoad':
        if 'Condition' in task:
            returnVal = checkCondition(task['Condition'])
            if  returnVal == 0:
                addLog(name," Skipped")
                return
            elif returnVal == -1:
                print("\n\nKey does not exist Yet!!!")
                while(1):
                    returnVal = checkCondition(task['Condition'])
                    if returnVal != -1:
                        break
                
        
        waitForInputs(task)
        
        
        
        
        
        extra = "DataLoad ("
        values_list = list(task['Inputs'].values())
        n = len(values_list)
        for i in range(n):
            extra += values_list[i]
            if i != n-1:
                extra += ","
        extra += ')'
        addLog(name," Executing ",extra)
        data_key = name + '.DataTable'
        defect_key = name + '.NoOfDefects'
        all_data[data_key],all_data[defect_key] = readCsv(task['Inputs']['Filename'])
        print("Added key",data_key)
        print("Added key",defect_key)
    
        
        
        
        


# In[ ]:





# In[11]:


def doTasksMod(name,task):
    if task['Type'] == 'Flow':
        addLog(name," Entry")
        #do it
        if task['Execution'] == 'Sequential':
            for subtaskName,subtask in task['Activities'].items():
                doTasksMod(name+"."+subtaskName,subtask)
        elif task['Execution'] == 'Concurrent':
            # do parallel task
            subtasks = []
            threadList = []
            for subtaskName,subtask in task['Activities'].items():
                t = threading.Thread(target = doTasksMod,args=(name+"."+subtaskName,subtask))
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
            
            
        


# In[12]:


for k,v in yaml_data.items():
    doTasksMod(k,v)


# In[13]:


with open(logFileName) as file:
    for line in file:
        print(line)


# In[ ]:




