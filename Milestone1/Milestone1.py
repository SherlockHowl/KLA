from collections import deque
import datetime
import yaml

with open('Milestone1A.yaml', 'r') as file:
    data = yaml.safe_load(file)
output = open("Milestone1A.txt", "w")

def execWorkFlow(tuple, parent, log, stack):
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
            
    else:
        function = content['Function']
        time = datetime.datetime.now()
        log.write(str(time)+";"+fullName+" Executing "+function+"()\n")
    time = datetime.datetime.now()
    log.write(str(time)+";"+fullName+" Exit\n")

for work in data.items():
    stack = deque()
    stack.append(data[0])
    execWorkFlow(work, "", output, stack)

output.close()