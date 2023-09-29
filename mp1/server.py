import socket
from _thread import *
import threading
import re
import os
import json


log_path = ''
root = "/home/colinz2/mp1"
# root = "/home/keyangx3/mp1"

for file in os.listdir(root):
    if file.endswith('.log'):
        log_path = os.path.join(root, file)

# get the name of log file
log_file_name = log_path.split('/')[-1]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((socket.gethostname(), 55554))
s.listen(1)

print("socket is listening")
ignore_case_flag = False
multiline_flag = False
dot_all = False

flag = 0

# find command in the log file
while True:
    data = []
    data.append(log_file_name)

    client_connection, client_addr = s.accept()
    payload = client_connection.recv(4096)
    payload = json.loads(payload.decode('utf-8'))
    command = payload[0]

    if payload[1] == "-i":
        ignore_case_flag = True
        flag |= re.IGNORECASE

    elif payload[1] == "-m":
        multiline_flag = True
        flag |= re.MULTILINE

    elif payload[1] == "-s":
        dot_all = True
        flag |= re.DOTALL

    # add in the flags
    with open(log_path, 'r') as log_file:
        # Iterate through each line in the log file
        string = ''

        count = 0
        iterate = 0
        for line in log_file:
            # Check if the line matches the pattern
            iterate += 1

            if re.search(command, line, flag):
                line = str(iterate) + ',' + line
                data.append(line)

                count += 1

        data.append(count)

    if data != []:
        print("data from server, ", data)
        results = json.dumps(data)
        client_connection.sendall(results.encode('utf-8'))
        print('server sent successful')
    else:
        data_to_send = "no data"
        client_connection.send(data_to_send.encode('utf-8'))

    client_connection.close()

    flag = 0

s.close()
