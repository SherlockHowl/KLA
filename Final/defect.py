import time
import csv

def checkPreced(new, old, preced):
    for index in range(len(preced)):
        if new == preced[index]:
            return True
        elif old == preced[index]:
            return False
    return True

def TimeFunction(ExecutionTime):
    time.sleep(ExecutionTime)

def DataLoad(input):
    file = open(input, "r")
    dataTable = csv.reader(file)
    data = []
    for row in dataTable:
        data.append(row)
    file.close()
    NoOfDefects = len(data)-1
    return data, NoOfDefects

def Binning(data, rules):
    if(len(data[0]) < 5):
        data[0].append('Bincode')
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
                    flag = flag and (int(data[index+1][3]) < thresh[ruleIndex])
                elif(cond[ruleIndex]) == '>':
                    flag = flag and (int(data[index+1][3]) > thresh[ruleIndex])
            if flag:
                if(len(data[index+1]) < 5):
                    data[index+1].append(BinID)
                else:
                    data[index+1][4] = BinID
    return data, dataSize
