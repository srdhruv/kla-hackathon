#!/usr/bin/env python
# coding: utf-8

# In[134]:


#56_Dhruv_Rajendra_Sawarkar_IITDM
import yaml
from datetime import datetime

import time
import threading
import os
import pandas as pd
import copy


# In[135]:


yaml_data = {}
example_fileName = "Milestone3/Milestone1_Example.yaml"
fileA = "Milestone3/Milestone3A.yaml"
fileB = "Milestone3/Milestone3B.yaml"
logFileName = "Milestone3/MileStone3A_Log.txt"
example = 0
filename = fileA
dataset_folder = "Milestone3/"

if example == 1:
    filename = example_fileName
with open(filename) as file:
    yaml_data = yaml.safe_load(file)


# In[136]:


# yaml_data


# In[137]:


all_data = {}
global root_logs
root_logs = ""
global history_done
history_done = {}


# In[138]:


#creating log file
with open(logFileName,"w") as file:
    file.write("")
def writeLogInFile(log):
    with open(logFileName,"a") as file:
        file.write(log)


# In[139]:


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
    


# In[140]:


def readCsv(filename):
    loc = dataset_folder + filename
    df = pd.read_csv(loc)
    noOfDefects = len(df)
    return df,noOfDefects


# In[141]:


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
            
def performBinning(inputs_dict):
    """
    
    501,Signal > 59 and Signal < 100
    0          1  2  3   4      5 6
    BIN_ID,RULE
    500,Signal < 60
    Signal > 59 and Signal < 100
    0      1  2  3   4      5  6
    
    """
    ruleFileName = dataset_folder +  inputs_dict['RuleFilename']
    dataset = copy.copy(all_data[inputs_dict['DataSet'][2:-1]])
    lines = []
    with open(ruleFileName) as rfile:
        for line in rfile:
            lines.append(line)
    
    temp = lines[1].split(',')
    binId = temp[0]
    temp = temp[1]
    temp = temp.split(" ")
    operator1 = '>'
    operator2 = '>'
    val1 = 1
    val2 = 1
#     print(temp,len(temp))]
        
    operator1 = temp[1]
    operator2 = temp[1]
    val1 = int(temp[2])
    val2 = int(temp[2])
    print("setting val1 as ",val1)
    
    if len(temp) > 3:
        operator2 = temp[5]
        val2 = int(temp[6])
    
        
        
    
    
    
    newCol = []
    
    for entry in dataset['Signal']:
        flag1 = False
        flag2 = False
        if operator1 == '>' and entry > val1:
#             print(entry,">",val1," what")
            flag1 = True
        elif operator1 == '<' and entry < val1:
            flag1 = True
            
        if operator2 == '>' and entry > val2:
            flag2 = True
        elif operator2 == '<' and entry < val2:
            flag2 = True
            
        if flag1 and flag2:
            newCol.append(binId)
        else:
            newCol.append("")
    dataset["Bincode"] = newCol
    newDf = dataset
#     print("\n\nthis is new Df,",newDf)
    return newDf,len(newDf)

def performMerge(inputs):
    precedenceFile_loc = dataset_folder + inputs['PrecedenceFile']
    plist = []
    with open(precedenceFile_loc) as pfile:
        for line in pfile:
            plist.append(line)
    plist = plist[0]
    plist = plist.split(' >> ')
    print(plist)
    priority = {}
    i = 0
    for val in plist:
        priority[val] = i
        i += 1
    datasets = []
    i = -1
    for k,v in inputs.items():
        i = i + 1
        if i == 0:
            continue
        key_d = v[2:-1]
        datasets.append(all_data[key_d])
        
    finaldf = datasets[0]
    selection = [[] for i in range(len(datasets[0]))]
    for DataSet in datasets:
        i = 0
        for entry in DataSet['Bincode']:
            if entry != '':
                selection[i].append(entry)
            i += 1
    
    newCol = []
    
    for lst in selection:
        flag = 0
        for higher in plist:
            for option in lst:
                if int(option) == int(higher):
                    newCol.append(int(option))
                    flag = 1
                    break
            if flag == 1:
                break
        if flag == 0:
            newCol.append(0)
    finaldf['Bincode'] = newCol
    print(finaldf)
    return finaldf,len(finaldf)

    
    
    


# In[142]:


def createResults(inputs):
    fileName = inputs['FileName']
    defectTableKey = inputs['DefectTable'][2:-1]
    defectTable = all_data[defectTableKey]
    defectTable.to_csv(dataset_folder+fileName,index=False)


# In[143]:


checkCondition("$(M2B_Workflow.TaskA.NoOfDefects) > 0")


# In[144]:


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
                if returnVal == 0:
                    addLog(name," Skipped")
                    return
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
                if returnVal == 0:
                    addLog(name," Skipped")
                    return
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
        
    elif task['Function'] == 'Binning':
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
                if returnVal == 0:
                    addLog(name," Skipped")
                    return
        waitForInputs(task)
        
        extra = "Binning ("
        values_list = list(task['Inputs'].values())
        n = len(values_list)
        for i in range(n):
            if values_list[i][0] == '$':
#                 extra += str(all_data[values_list[i][2:-1]])
#             else:
                extra += values_list[i]
            if i != n-1:
                extra += ","
        extra += ')'
        addLog(name," Executing ",extra)
        
                
        data_key = name + ".BinningResultsTable"
        defect_key = name + ".NoOfDefects"
        all_data[data_key],all_data[defect_key] = performBinning(task['Inputs'])
        print("Added key",data_key)
        print("Added key",defect_key)
    elif task['Function'] == "MergeResults":
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
                if returnVal == 0:
                    addLog(name," Skipped")
                    return
        waitForInputs(task)
        
        data_key = name + ".MergedResults"
        defect_key = name + ".NoOfDefects"
        all_data[data_key],all_data[defect_key] = performMerge(task['Inputs'])
        
    elif task['Function'] == 'ExportResults':
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
                if returnVal == 0:
                    addLog(name," Skipped")
                    return
        waitForInputs(task)
        
        createResults(task['Inputs'])
        
        
        
    
        
        
        
        


# In[145]:


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
            
            
        


# In[146]:


for k,v in yaml_data.items():
    doTasksMod(k,v)


# In[147]:


with open(logFileName) as file:
    for line in file:
        print(line)


# In[148]:


# M3A_Workflow.BinningProcess.BinningFor500.BinningResultsTable
# M3A_Workflow.BinningProcess.BinningFor500.BinningResultsTable


# In[ ]:





# In[ ]:




