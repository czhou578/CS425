import socket
import os
from _thread import *
import threading
import sys
import json
import time

start_time = time.time()

if len(sys.argv) == 2:
    query = sys.argv[1]
    flag = ''
elif len(sys.argv) == 3:
    query = sys.argv[1]
    flag = sys.argv[2]

port = 55554
hosts = [
    'fa23-cs425-2901.cs.illinois.edu',
    'fa23-cs425-2902.cs.illinois.edu',
    'fa23-cs425-2903.cs.illinois.edu',
    'fa23-cs425-2904.cs.illinois.edu',
    'fa23-cs425-2905.cs.illinois.edu',
    'fa23-cs425-2906.cs.illinois.edu',
    'fa23-cs425-2907.cs.illinois.edu',
    'fa23-cs425-2908.cs.illinois.edu',
    'fa23-cs425-2909.cs.illinois.edu',
    'fa23-cs425-2910.cs.illinois.edu'
]


total_sum = 0
total_time = 0.0
lock = threading.Lock()

# Create a socket object
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print("Socket successfully created")


class MyThread(threading.Thread):
    def __init__(self, host, port, pattern):
        super().__init__()
        self.host = host
        self.port = port
        self.pattern = pattern
        self.result = None
        self.connected = False

    def run(self):
        global total_sum
        global total_time
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                print('connection made')
                self.connected = True
                s.send(json.dumps([query, flag]).encode('utf-8'))

                matching_lines = []

                while True:
                    temp = b''
                    while True:
                        received_data = s.recv(4096)

                        if not received_data:
                            break

                        temp += received_data

                    data = json.loads(temp.decode('utf-8'))
                    matching_lines.append(data)

                    print(f"{matching_lines[0][0]} has total",
                          matching_lines[0][-1], "matching lines")

                    with lock:
                        total_sum += matching_lines[0][-1]

                    for i in range(len(matching_lines[0]) - 1):
                        if i == 0:
                            print("This is the name of log file:",
                                  matching_lines[0][i])
                        else:
                            result = matching_lines[0][i].split(',')
                            print("The index of this line is:", result[0])
                            print("The content of this line is:", result[1])

                    break

        except Exception as e:
            print(str(e))


# Establish connection with client.
while True:

    threads = []

    for host in hosts:
        worker = MyThread(host, port, query)
        threads.append(worker)
        worker.start()

        if worker.connected == False:
            continue

    for worker in threads:
        worker.join()

    end_time = time.time()
    exec_time = end_time - start_time

    print("Total number of matching lines: ", total_sum)
    print("Total time spent: ", exec_time)

    break

s.close()
