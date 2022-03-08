from concurrent.futures import thread
import threading
import datetime
import time
from nbformat import write
import yaml

with open('Milestone1B.yaml', 'r') as file:
    data = yaml.safe_load(file)
output = open("Milestone1B.txt", "w")

def TimeFunction(ExecutionTime):
    time.sleep(ExecutionTime)

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
            execTime = content['Inputs']['ExecutionTime']
            execTime = int(execTime)
            TimeFunction(execTime)
            log.write(str(time)+";"+fullName+" Executing "+function+"(")
            flag = False
            for input in content['Inputs']:
                if(flag):
                    log.write(",")
                log.write(content['Inputs'][input])
                flag = True
            log.write(")\n")
        else:
            log.write(str(time)+";"+fullName+" Executing "+function+"()\n")
    time = datetime.datetime.now()
    log.write(str(time)+";"+fullName+" Exit\n")

for work in data.items():
    execWorkFlow(work, "", output)

output.close()