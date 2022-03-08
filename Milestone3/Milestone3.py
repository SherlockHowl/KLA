from concurrent.futures import thread
import threading
import datetime
import time
import csv
from numpy import append
import yaml

outputStack = dict()
outputEvent = dict()

with open('Milestone3A.yaml', 'r') as file:
    data = yaml.safe_load(file)
output = open("Milestone3A.txt", "w")

def parseInput(input):
    if(input[0] == '$'):
        key = input[2:-1]
        if(key not in outputEvent):
            outputEvent[key] = threading.Event()
        outputEvent[key].wait(0)
        valueOfKey = outputStack[key]
        return valueOfKey
    else:
        return input

def CheckCondition(condition):
    condition = condition.split()
    key = condition[0]
    valueOfKey = parseInput(key)
    if(key[0] == '$'):
        key = key[2:-1]
        if(key not in outputEvent):
            outputEvent[key] = threading.Event()
        outputEvent[key].wait(0)
        valueOfKey = outputStack[key]
    else:
        valueOfKey = int(key)
    key = condition[2]
    thresh = 0
    if(key[0] == '$'):
        key = key[2:-1]
        if(key not in outputEvent):
            outputEvent[key] = threading.Event()
        outputEvent[key].wait(0)
        thresh = outputStack[key]
    else:
        thresh = int(key)
    if (condition[1] == '<'):
        return (valueOfKey < thresh)
    elif (condition[1] == '>'):
        return (valueOfKey > thresh)
    elif (condition[1] == '=>'):
        return (valueOfKey >= thresh)
    elif (condition[1] == '<='):
        return (valueOfKey <= thresh)
    elif (condition[1] == '=='):
        return (valueOfKey == thresh)

def TimeFunction(ExecutionTime):
    time.sleep(ExecutionTime)

def HandleTimeFunction(inputs, fullName, time, log):
    key = inputs['FunctionInput']
    out = key
    if(key[0] == '$'):
        key = key[2:-1]
        if(key not in outputEvent):
            outputEvent[key] = threading.Event()
        outputEvent[key].wait(0)
        out = outputStack[key]
        out = str(out)
    execTime = inputs['ExecutionTime']
    execTime = int(execTime)
    TimeFunction(execTime)
    log.write(str(time)+";"+fullName+" Executing "+"TimeFunction(")
    log.write(out+","+str(execTime))
    log.write(")\n")

def DataLoad(input):
    file = open(input, "r")
    dataTable = csv.reader(file)
    data = []
    for row in dataTable:
        data.append(row)
    file.close()
    NoOfDefects = len(data)-1
    return data, NoOfDefects

def HandleDataLoad(input, id, time, log):
    DataTable, NoOfDefects = DataLoad(input)
    log.write(str(time)+";"+id+" Executing DataLoad("+input+")\n")
    if(id+".DataTable" not in outputEvent):
        outputEvent[id+".DataTable"] = threading.Event()
    if(id+".NoOfDefects" not in outputEvent):
        outputEvent[id+".NoOfDefects"] = threading.Event()
    outputStack[id+".DataTable"] = DataTable
    outputEvent[id+".DataTable"].set()
    outputStack[id+".NoOfDefects"] = NoOfDefects
    outputEvent[id+".NoOfDefects"].set()

def Binning(data, rules):
    dataSize = len(data) - 1
    for row in rules:
        row = row.split(',')
        if(len(row) < 2):
            continue
        BinID = row[0]
        rule = row[1].split()
        size = len(rule)
        size = int((size+1)/4)
        variable = []
        cond = []
        thresh = []
        count = 0
        shift = 0
        while(count<size):
            variable.append(rule[shift+0])
            cond.append(rule[shift+1])
            thresh.append(int(rule[shift+2]))
            count = count + 1
            shift = shift + 4
        for index in range(dataSize):
            flag = True
            for ruleIndex in range(len(cond)):
                if(cond[ruleIndex]) == '<':
                    flag = (int(data[index+1][3]) < thresh[ruleIndex])
                elif(cond[ruleIndex]) == '>':
                    flag = (int(data[index+1][3]) > thresh[ruleIndex])
            if flag:
                if(len(data[index]) < 5):
                    data[index].append(BinID)
                else:
                    data[index][4] = BinID
    return data, dataSize

def HandleBinning(inputs, fullName, time, log):
    DataSet1 = parseInput(inputs['DataSet'])
    ruleFile = open(inputs['RuleFilename'], 'r')
    rules = csv.reader(ruleFile)
    next(rules)
    log.write(str(time)+";"+fullName+" Executing Binning("+inputs['RuleFilename']+")\n")
    BinningResultsTable,NoOfDefects = Binning(DataSet1.copy(), ruleFile)
    if(fullName+".BinningResultsTable" not in outputEvent):
        outputEvent[fullName+".BinningResultsTable"] = threading.Event()
    outputStack[fullName+".BinningResultsTable"] = BinningResultsTable
    outputEvent[fullName+".BinningResultsTable"].set()
    if(fullName+".NoOfDefects" not in outputEvent):
        outputEvent[fullName+".NoOfDefects"] = threading.Event()
    outputStack[fullName+".NoOfDefects"] = BinningResultsTable
    outputEvent[fullName+".NoOfDefects"].set()
    ruleFile.close()

def HandleMergeResults(inputs, fullName, time, log):
    

def execWorkFlow(tuple, parent, log):
    time = datetime.datetime.now()
    name = tuple[0]
    fullName = ""
    if parent == "":
        fullName = name
    else:
        fullName = parent+"."+name
    log.write(str(time)+";"+fullName+" Entry\n")
    content = tuple[1]
    if 'Condition' in content:
        skip = CheckCondition(content['Condition'])
        if(skip == False):
            log.write(str(time)+";"+fullName+" Skipped\n")
            log.write(str(time)+";"+fullName+" Exit\n")
            return
    if(content['Type'] == 'Flow'):
        if(content['Execution'] == 'Sequential'):
            for act in content['Activities'].items():
                execWorkFlow(act, fullName, log)
        else:
            size = len(content['Activities'])
            threads = []
            for act in content['Activities'].items():
                temp_thread = threading.Thread(target=execWorkFlow, args=(act, fullName, log,))
                temp_thread.start()
                threads.append(temp_thread)
            for t in threads:
                t.join()
    else:
        function = content['Function']
        time = datetime.datetime.now()
        if(function == 'TimeFunction'):
            HandleTimeFunction(content['Inputs'], fullName, time, log)
        elif(function == 'DataLoad'):
            HandleDataLoad(content['Inputs']['Filename'], fullName, time, log)
        elif(function == 'Binning'):
            HandleBinning(content['Inputs'], fullName, time, log)
        elif(function == 'MergeResults'):
            HandleMergeResults(content['Inputs'], fullName, time, log)
    time = datetime.datetime.now()
    log.write(str(time)+";"+fullName+" Exit\n")

for work in data.items():
    execWorkFlow(work, "", output)

output.close()