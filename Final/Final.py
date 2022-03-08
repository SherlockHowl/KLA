import threading
import datetime
import csv
import yaml
from defect import TimeFunction, DataLoad, Binning, checkPreced

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
    outputStack[fullName+".NoOfDefects"] = NoOfDefects
    outputEvent[fullName+".NoOfDefects"].set()
    ruleFile.close()

def HandleMergeResults(inputs, fullName):
    precedFile = inputs['PrecedenceFile']
    precedFile = open(precedFile, 'r')
    precedence = precedFile.readline()
    precedence = precedence.split()
    preced = []
    count = 0
    merge = []
    while(count < len(precedence)):
        preced.append(precedence[count])
        count = count + 2
    for key in inputs:
        if key != "PrecedenceFile":
            tempData = parseInput(inputs[key])
            if len(merge) == 0:
                merge = tempData.copy()
                continue
            dataSize = len(tempData) -1
            for index in range(dataSize):
                if len(tempData[index+1]) < 5:
                    continue
                if len(merge[index+1]) < 5:
                    merge[index+1].append(tempData[index+1][4])
                else:
                    check = checkPreced(merge[index+1][4], tempData[index+1][4], preced)
                    if check:
                        merge[index+1][4] = tempData[index+1][4]
    if(fullName+".MergedResults" not in outputEvent):
        outputEvent[fullName+".MergedResults"] = threading.Event()
    outputStack[fullName+".MergedResults"] = merge
    outputEvent[fullName+".MergedResults"].set()
    if(fullName+".NoOfDefects" not in outputEvent):
        outputEvent[fullName+".NoOfDefects"] = threading.Event()
    outputStack[fullName+".NoOfDefects"] = dataSize
    outputEvent[fullName+".NoOfDefects"].set()
    precedFile.close()

def HandleExportResults(inputs):
    filename = inputs['FileName']
    rows = inputs['DefectTable']
    rows = parseInput(rows)
    with open(filename, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(rows)

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
            HandleMergeResults(content['Inputs'], fullName)
        elif(function == 'ExportResults'):
            HandleExportResults(content['Inputs'])
    time = datetime.datetime.now()
    log.write(str(time)+";"+fullName+" Exit\n")

for work in data.items():
    execWorkFlow(work, "", output)

output.close()