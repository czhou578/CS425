from fail_detector import Fail_Dector_Server
import subprocess
import socket
import queue
import random
import json
import time
import os
import threading
import argparse
import copy
import shutil
import glob
import time

# NAME = "keyangx3"
NAME = "colinz2"
SDFS_PATH = "sdfs"
DEFAULT_PORT_NUM = 55555
REPLICATION_FACTOR = 4
LEADER_MACHINE = 'fa23-cs425-2901.cs.illinois.edu'

ALL_HOSTS = [
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

class File_Tracker:
    def __init__(self):
        self.file_versions = {} #file names map to most updated version number
        self.file_node = {i + 1: set() for i in range(10)} #machine id map to filenames stored in that machine id
        self.leader_node = 'fa23-cs425-2901.cs.illinois.edu'

class Server(Fail_Dector_Server):
    def __init__(self, args):
        super().__init__(args)
        self.host = socket.gethostname()
        self.port = DEFAULT_PORT_NUM
        self.file_tracker = File_Tracker()
        self.queue = queue.Queue(maxsize=10)  # Queue to handle incoming requests
        self.replication_factor = 4
        self.inputLock = threading.Lock()
        self.sdfsFolder = 'sdfs/'
        self.leaderMachine = LEADER_MACHINE
        self.first_time_delete = True
        self.rep_target_machines = [] # machines waiting for ACK reply when replicating
        self.fail_but_not_process = [] # failed vm that is waiting for its replication to finish
        self.replication_time = 0 # tracking the number of puts needed for replication 

    def node_assigned_replicas(self, node_num): #get assigned replicas for a node
        replicas = []
        for i in range(2):
            replicas.append(((int(node_num) + i) % 10) + 1)
        
        print('replica numbers, ', replicas)
        
        return replicas
    
    def get_machine_num(self, host): #get the machine number
        return host[14] if host[14] != '0' else 10
    
    def remove_files_join(self): #remove files when joining
        file_list = os.listdir(self.sdfsFolder)
        
        # Iterate through the files and remove them
        for file_name in file_list:
            file_path = os.path.join(self.sdfsFolder, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)

    def elect_leader(self): #elect new leader
        temp = []
        while True:
            if self.latest_failed_members != []:
                temp = self.latest_failed_members

                for failed_host in self.latest_failed_members:
                    if self.leaderMachine[0:15] in failed_host:
                        current_alive_machines = super().getMembershipListID()
                        int_working_host_list = sorted(current_alive_machines, reverse=True) #set of remaining membership id's

                        if (int(self.host[14]) == int_working_host_list[0]) or self.host == 'fa23-cs425-2910.cs.illinois.edu': #greatest alive host number
                            for host in ALL_HOSTS:
                                try:
                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                        s.connect((host, self.port))
                                        leader_message = {
                                            'status': 'LEADER_UPDATE',
                                            'new_leader': self.host
                                        }

                                        s.sendall(json.dumps(leader_message).encode('utf-8'))
                                        print('leader elect message sent from')
                                except Exception as e:
                                    print(str(e))

    def Starvation(self): #deal with starvation 
        while True:
            if self.leaderMachine == self.host:
                time.sleep(1)
                if not self.queue.empty():
                    list_as_line = json.dumps(list(self.queue.queue))
                    with open('queue.log', 'a') as log_file:
                        log_file.write(list_as_line + "\n")              
                ## check starvation of the queue
                if self.queue.qsize() <= 4:
                    continue
                else:
                    queue_elements = list(self.queue.queue)
                    
                    i = 0
                    while i < len(queue_elements) - 4:
                        if queue_elements[i]["sdfs_file_name"] == queue_elements[i+1]["sdfs_file_name"] == queue_elements[i+2]["sdfs_file_name"] == queue_elements[i+3]["sdfs_file_name"] == queue_elements[i+4]["sdfs_file_name"]:
                            if queue_elements[i]["command"] in ["PUT", "DELETE"] and queue_elements[i+1]["command"] in ["PUT", "DELETE"] and queue_elements[i+2]["command"] in ["PUT", "DELETE"] and queue_elements[i+3]["command"] in ["PUT", "DELETE"]:
                                for j in range(i+4, self.queue.qsize()):
                                    if self.queue.queue[j]["command"] == "GET":
                                        element_i = self.queue.queue[i+3]
                                        element_j = self.queue.queue[j]
                                        # Swap the elements
                                        self.queue.queue[i+3] = element_j
                                        self.queue.queue[j] = element_i
                                        break

                            elif queue_elements[i]["command"] == "GET" and queue_elements[i+1]["command"] == "GET" and queue_elements[i+2]["command"] == "GET" and queue_elements[i+3]["command"] == "GET":
                                for j in range(i+4, self.queue.qsize()):
                                    if self.queue.queue[j]["command"] in ["PUT", "DELETE"]:
                                        element_i = self.queue.queue[i+3]
                                        element_j = self.queue.queue[j]

                                        # Swap the elements
                                        self.queue.queue[i+3] = element_j
                                        self.queue.queue[j] = element_i
                                        break
                            i = i + 1                                    

    def queue_logic(self): #logic for the queue
        count = 1
        while True:
            if self.host == self.leaderMachine:
                time.sleep(1)
                parallel_execution = []
                file_access = {}
                if self.queue.qsize() == 1:
                    data = self.queue.get()
                    file_access[data["sdfs_file_name"]] = []
                    file_access[data["sdfs_file_name"]].append(data["command"])
                    parallel_execution.append(data)

                    threads = []
                    
                    for data in parallel_execution:
                        if data["command"] == "GET":
                            thread = threading.Thread(target=self.get, args=(data,))
                        elif data["command"] == "PUT":
                            thread = threading.Thread(target=self.put, args=(data,))
                        elif data["command"] == "DELETE":
                            thread = threading.Thread(target=self.delete, args=(data,))
                        threads.append(thread)
                        thread.start()

                    # Wait for all threads to finish
                    for thread in threads:
                        thread.join()

                    # print("ROUND " + str(count))
                    count += 1
                    
                elif self.queue.qsize() > 1:
                    while self.queue.qsize() > 0:
                        # print("current queue size: " + str(self.queue.qsize()))
                        if self.queue.qsize() > 1:
                            if self.queue.queue[0]["sdfs_file_name"] != self.queue.queue[1]["sdfs_file_name"]:
                                if self.queue.queue[0]["sdfs_file_name"] in file_access:
                                    if self.queue.queue[0]["command"] == "GET" and file_access[self.queue.queue[0]["sdfs_file_name"]].count("GET") <= 1:
                                        data = self.queue.get()
                                        file_access[data["sdfs_file_name"]].append(data["command"])
                                        parallel_execution.append(data)
                                    else:
                                        break
                                else:
                                    data = self.queue.get()
                                    file_access[data["sdfs_file_name"]] = []
                                    file_access[data["sdfs_file_name"]].append(data["command"])
                                    parallel_execution.append(data)

                            else:
                                if self.queue.queue[0]["command"] == "GET" and self.queue.queue[1]["command"] == "GET":
                                    if self.queue.queue[0]["sdfs_file_name"] in file_access:
                                        break
                                    else:
                                        data0 = self.queue.get()
                                        data1 = self.queue.get()

                                        file_access[data0["sdfs_file_name"]] = []
                                        file_access[data0["sdfs_file_name"]].append(data0["command"])
                                        file_access[data0["sdfs_file_name"]].append(data0["command"])

                                        parallel_execution.append(data0)
                                        parallel_execution.append(data1)
                                else:
                                    if self.queue.queue[0]["sdfs_file_name"] in file_access:
                                        break
                                    
                                    else:
                                        data = self.queue.get()
                                        file_access[data["sdfs_file_name"]] = []
                                        file_access[data["sdfs_file_name"]].append(data["command"])
                                        parallel_execution.append(data)
                                        break

                        elif self.queue.qsize() == 1:
                            if self.queue.queue[0]["sdfs_file_name"] not in file_access:
                                data = self.queue.get()
                                parallel_execution.append(data)
                                break
                            else:
                                if self.queue.queue[0]["command"] == "GET" and file_access[self.queue.queue[0]["sdfs_file_name"]].count("GET") <= 1:
                                    data = self.queue.get()
                                    parallel_execution.append(data)
                                    break
                                else:
                                    if self.queue.queue[0]["command"] == "GET" and file_access[self.queue.queue[0]["sdfs_file_name"]].count("GET") <= 1:
                                        data = self.queue.get()
                                        file_access[data["sdfs_file_name"]].append(data["command"])
                                        parallel_execution.append(data)
                                        break
                                    else:
                                        break

                    threads = []
                    
                    for data in parallel_execution:
                        if data["command"] == "GET":
                            thread = threading.Thread(target=self.get, args=(data,))
                        elif data["command"] == "PUT":
                            thread = threading.Thread(target=self.put, args=(data,))
                        elif data["command"] == "DELETE":
                            thread = threading.Thread(target=self.delete, args=(data,))
                        threads.append(thread)
                        thread.start()

                    # Wait for all threads to finish
                    for thread in threads:
                        thread.join()

                count += 1
  
    def put(self, data):
        file_machine_tracker = self.file_tracker.file_node
        file_versions = self.file_tracker.file_versions
        host_num = int(self.get_machine_num(self.host))        
        assigned_replicas = None

        if "node_num" in data:
            assigned_replicas = self.node_assigned_replicas(data["node_num"]) #find assigned machines to put
            for replica_num in assigned_replicas:

                if replica_num != 10:
                    prefix = f"{NAME}@fa23-cs425-290%d.cs.illinois.edu:/home/{NAME}/mp3" % (replica_num)
                    p = subprocess.Popen(['scp', f'sdfs/{data["sdfs_file_name"]}-{str(data["version"])}', prefix + '/' + os.path.join(SDFS_PATH, data["sdfs_file_name"]) + f'-{str(data["version"])}'])
                    return_code = p.wait()

                    if return_code == 0:
                        file_machine_tracker[replica_num].add(f'{data["sdfs_file_name"]}-{str(data["version"])}')
                        file_versions[data["sdfs_file_name"]] = data["version"]
                        file_machine_tracker[host_num].add(f'{data["sdfs_file_name"]}-{str(data["version"])}')
                    
                else:
                    prefix = f"{NAME}@fa23-cs425-2910.cs.illinois.edu:/home/{NAME}/mp3"
                    p = subprocess.Popen(['scp', f'sdfs/{data["sdfs_file_name"]}-{str(data["version"])}', prefix + '/' + os.path.join(SDFS_PATH, data["sdfs_file_name"]) + f'-{str(data["version"])}'])
                    return_code = p.wait()

                    if return_code == 0:
                        file_machine_tracker[replica_num].add(f'{data["sdfs_file_name"]}-{str(data["version"])}')
                        file_versions[data["sdfs_file_name"]] = data["version"] 
                        file_machine_tracker[host_num].add(f'{data["sdfs_file_name"]}-{str(data["version"])}')

            my_dict_as_lists = copy.deepcopy(file_machine_tracker)            

            my_dict_as_lists = {key: list(value) for key, value in my_dict_as_lists.items()}
            
            for host in ALL_HOSTS:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((host, self.port))
                        if "command" in data:
                            msg = {
                            'status': 'PUT',
                            'file-versions': file_versions,
                            'file-machines': my_dict_as_lists,
                            # 'machineNum': int(machine_num)
                            }
                            s.sendall(json.dumps(msg).encode('utf-8'))

                except Exception as e:
                    print(str(e))

        else:
            
            target_machine = data['target_machine']
            if target_machine != 10:
                prefix = f"{NAME}@fa23-cs425-290%d.cs.illinois.edu:/home/{NAME}/mp3" % (target_machine)
                
                p = subprocess.Popen(['scp', f'sdfs/{data["sdfs_file_name"]}', prefix + '/' + os.path.join(SDFS_PATH, data["sdfs_file_name"])])
                return_code = p.wait()

                if return_code == 0:
                    file_machine_tracker[target_machine].add(f'{data["sdfs_file_name"]}')
                
            else:
                prefix = f"{NAME}@fa23-cs425-2910.cs.illinois.edu:/home/{NAME}/mp3"

                p = subprocess.Popen(['scp', f'sdfs/{data["sdfs_file_name"]}', prefix + '/' + os.path.join(SDFS_PATH, data["sdfs_file_name"])])
                return_code = p.wait()

                if return_code == 0:
                    file_machine_tracker[target_machine].add(f'{data["sdfs_file_name"]}')

            my_dict_as_lists = copy.deepcopy(file_machine_tracker)            

            my_dict_as_lists = {key: list(value) for key, value in my_dict_as_lists.items()}

            for host in ALL_HOSTS: #send the updated file tracker to everyone
                host_id = self.get_machine_num(host)
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((host, self.port))
                        if "command" in data:
                            msg = {
                            'status': 'PUT',
                            'file-versions': file_versions,
                            'file-machines': my_dict_as_lists,
                            # 'machineNum': int(machine_num)
                            }
                            s.sendall(json.dumps(msg).encode('utf-8'))

                except Exception as e:
                    print(str(e)) 
        
            self.rep_target_machines.remove(target_machine)

        print('Put done')

    def get(self, data):
        sdfs_file_name = data["sdfs_file_name"] 
        file_versions = self.file_tracker.file_versions
    
        if sdfs_file_name not in file_versions:
            print('this file is not in the system currently')
            return
        else:
            machine_num = int(data["node_num"])
            version_num = self.file_tracker.file_versions[sdfs_file_name]
            if machine_num != 10:
                prefix = f"{NAME}@fa23-cs425-290%d.cs.illinois.edu:/home/{NAME}/mp3" % (machine_num)
                p = subprocess.Popen(['scp', f'sdfs/{sdfs_file_name}-{version_num}', prefix + '/' + sdfs_file_name])
                return_code = p.wait()

                if return_code == 0:
                    host = 'fa23-cs425-290%d.cs.illinois.edu' % (machine_num)
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((host, self.port))
                            msg = {
                                'status': 'GET FINISHED',
                                # 'machineNum': int(machine_num)
                            }
                            s.sendall(json.dumps(msg).encode('utf-8'))

                    except Exception as e:
                        print(str(e))
            else:
                prefix = f"{NAME}@fa23-cs425-2910.cs.illinois.edu:/home/{NAME}/mp3"
                p = subprocess.Popen(['scp', f'sdfs/{sdfs_file_name}-{version_num}', prefix + '/' + sdfs_file_name])
            
            print("GET" + str(data["node_num"]))


    def delete(self, data):
        file_list = os.listdir(self.sdfsFolder)
        file_machine_tracker = self.file_tracker.file_node
        file_versions = self.file_tracker.file_versions
        
        # Iterate through the files and remove them
        for file_name in file_list:
            temp_file_name = file_name.split('-')
            if data["sdfs_file_name"] == temp_file_name[0]:
                file_path = os.path.join(self.sdfsFolder, f'{data["sdfs_file_name"]}-{temp_file_name[1]}')
                os.remove(file_path)
                if data["sdfs_file_name"] in file_versions:
                    del file_versions[data["sdfs_file_name"]]

        machine_num = self.get_machine_num(self.host)
        for key, value in file_machine_tracker.items():
            file_machine_tracker[key] = {item for item in value if data["sdfs_file_name"] not in item}

        json_file_machine_tracker = file_machine_tracker[int(machine_num)] #copy the set
        json_file_machine_tracker = list(json_file_machine_tracker) #turn set into a list

        if self.host == self.leaderMachine:
            for host in ALL_HOSTS:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((host, self.port))
                        msg = {
                            'status': 'DELETE',
                            'file-versions': file_versions,
                            'file-machines': json_file_machine_tracker,
                            'machineNum': int(machine_num),
                            'sdfs_file_name': data["sdfs_file_name"]
                        } 

                        s.sendall(json.dumps(msg).encode('utf-8'))
                except Exception as e:
                    print(str(e))
        else:
            print(f'deleted file {data["sdfs_file_name"]} from machine {self.host}')

    def rereplicate(self):
        temp = []
        while True:
            if self.host == self.leaderMachine:

                if self.latest_failed_members != temp or self.fail_but_not_process != []:
                    temp = self.latest_failed_members

                    temp_copy = copy.deepcopy(self.latest_failed_members)
                    int_failed_host_list = []
                    int_working_host_list = super().getMembershipListID() #set of membership id's
                    # print(int_working_host_list)
                    for failed_host in temp_copy: #add host integer number to list
                        if 'fa23-cs425-2910' in failed_host:
                            int_failed_host_list.append(10)
                        else:
                            int_failed_host_list.append(int(failed_host[14]))
                    
                    # print('these hosts have failed, ', int_failed_host_list)
                    # print(self.rep_target_machines)
                    # print('need to be processed' + str(self.fail_but_not_process))
                    if self.rep_target_machines == [] and self.fail_but_not_process != []:
                        int_failed_host_list = self.fail_but_not_process
                        int_working_host_list.difference_update(int_failed_host_list) # subtract hosts that have failed
                        # print('working hosts after subtracting failed hosts, ', int_working_host_list)
                        possible_hosts = int_working_host_list
                        
                        for host_num in int_failed_host_list: #loop through every failed host
                            # print('before error, ', self.file_tracker.file_node)
                            file_node_entries = list(self.file_tracker.file_node[host_num]) #list of filenames on failed machine

                            self.file_tracker.file_node[host_num].clear()

                            for file_name in file_node_entries:
                                # print('possible hosts in file_name, ', possible_hosts)
                                copy_possible_hosts = copy.copy(possible_hosts)

                                # print('working hosts after subtracting failed hosts, ', int_working_host_list)
                                # print(f'possible hosts to insert {file_name}, ', copy_possible_hosts)

                                num_target_machines = 0
                                target_hosts_id = []

                                hosts_with_file = self.find_machines_with_file(file_name, self.file_tracker.file_node, host_num)
                        
                                if len(hosts_with_file) == REPLICATION_FACTOR: 
                                    continue
                                else:
                                    num_target_machines = REPLICATION_FACTOR - len(hosts_with_file)
                                    # print(f'num target machines to insert {file_name}, ', num_target_machines)

                                copy_possible_hosts.difference_update(hosts_with_file)
                                # print(f'possible machines to insert, {file_name}', copy_possible_hosts)

                                if not copy_possible_hosts: continue

                                while num_target_machines > 0:
                                    target_machine_num = random.choice(list(copy_possible_hosts))
                                    if target_machine_num not in target_hosts_id:
                                        target_hosts_id.append(target_machine_num)
                                        copy_possible_hosts.remove(target_machine_num)
                                        num_target_machines -= 1

                                # print(f'target machine ids to insert {file_name}, ', target_hosts_id)

                                version_num = self.file_tracker.file_versions[file_name.split("-")[0]]
                                self.replication_time = len(target_hosts_id)
                                # print("vms prepare to replicate" + str(target_hosts_id))
                                for host_id in target_hosts_id:
                                
                                    self.rep_target_machines.append(host_id) #**********************

                                   
                                    update_msg = {
                                        'command': 'PUT',
                                        'sdfs_file_name': file_name,
                                        'target_machine': host_id,
                                        'version': version_num
                                    }

                                    self.queue.put(update_msg)

                        del self.fail_but_not_process[0]                
                                    
                    else:
                        # print("CHECK:" + str(int_failed_host_list))
                        if int_failed_host_list != []:
                            for item in int_failed_host_list:
                                if self.fail_but_not_process.count(item) == 0:
                                    self.fail_but_not_process.append(item)

    def receiver(self): #listen for incoming messages to update filetable
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen(1)

            total_bytes_received = 0

            while True:
                conn, addr = s.accept()
                payload = conn.recv(4096)
                if not payload:
                    break
            
                data = json.loads(payload.decode('utf-8'))
                print('data at receiver, ', data)
                
                if "command" in data and self.host == self.leaderMachine:
                    if data["command"] == 'PUT':

                        sent_machine_tracker_entry = data['file-machines']
                        self.file_tracker.file_node[int(data['node_num'])].update(sent_machine_tracker_entry)
                        print('receiver set11, ', self.file_tracker.file_node)                        

                    self.queue.put(data)

                if self.host == self.leaderMachine:
                    if "command" not in data:
                        #pass
                        if data['status'] == 'PUT':
                            my_set = {int(key): set(value) for key, value in data['file-machines'].items()}
                            self.file_tracker.file_versions = data['file-versions']
                            self.file_tracker.file_node = my_set
                        
                        elif data['status'] == 'PUT-ACK':
                            if len(self.rep_target_machines) != 0 and data['machine-num'] in self.rep_target_machines:
                                self.rep_target_machines.remove(int(data['machine-num']))
                            
                            if len(self.rep_target_machines) == 0:
                                self.waiting_rep_ack_flag = False
                        
                else:
                    print('not main host')
                    if data['status'] == 'PUT':
                        self.file_tracker.file_versions = data['file-versions']
                        my_set = {int(key): set(value) for key, value in data['file-machines'].items()}

                        # num = self.get_machine_num(self.host)
                        # num = int(num)
                        # if next(iter(data['file-versions'])) not in my_set[num]:
                        #     if next(iter(data['file-versions'])) in self.file_tracker.file_versions:
                        #         key = next(iter(data['file-versions'])) 
                        #         my_set[num] = my_set[num] | {(key + '-' + str(data['file-versions'][key]))}
                        
                        self.file_tracker.file_node.update(my_set)

                        print('PUT FINISHED')

                    elif data['status'] == 'GET FINISHED':

                        print('GET IS FINISHED')
                    
                    elif data['status'] == 'DELETE':
                        self.file_tracker.file_node[int(data['machineNum'])].update(data['file-machines'])
                        self.file_tracker.file_versions = data['file-versions']
                        self.delete(data)
                              

                    
                    elif data['status'] == 'LEADER_UPDATE':
                        self.leaderMachine = data['new_leader']
                    
                    elif data['status'] == 'multi_get':
                        self.send_to_leader('get', None, data['sdfs_file_name'])

    def send_to_leader(self, command, file_name, sdfs_file_name): #send request to leader
        if command == 'put':
            machine_num = self.get_machine_num(self.host)
            prefix = f"{NAME}@{self.leaderMachine}:/home/{NAME}/mp3"

            version_num = 0

            if sdfs_file_name not in self.file_tracker.file_versions:
                self.file_tracker.file_versions[sdfs_file_name] = 0
            else:
                version_num = self.file_tracker.file_versions[sdfs_file_name] + 1
            
            self.file_tracker.file_node[int(machine_num)].add(sdfs_file_name + f'-{version_num}')
            print('in send to leader, ', self.file_tracker.file_node)

            copied_file_path = os.path.join(self.sdfsFolder, sdfs_file_name + f'-{version_num}')
            shutil.copy(file_name, copied_file_path)

            # Rename the copied file to the new name
            os.rename(copied_file_path, os.path.join(self.sdfsFolder, sdfs_file_name + f'-{version_num}'))       

            p = subprocess.Popen(['scp', f'sdfs/{sdfs_file_name}-{version_num}', prefix + '/' + os.path.join(SDFS_PATH, sdfs_file_name) + f'-{version_num}'])
            return_code = p.wait()

            my_dict_as_lists = {key: list(value) for key, value in self.file_tracker.file_node.items()}

            if return_code == 0:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.leaderMachine, self.port))
                        update_message = {
                            'command': command.upper(),
                            'node_num': machine_num,
                            'version': version_num,
                            'sdfs_file_name': sdfs_file_name,
                            'file-machines': my_dict_as_lists[int(machine_num)] 
                        }
                        s.sendall(json.dumps(update_message).encode('utf-8'))
                except Exception as e:  
                    print(str(e))
            
        elif command == 'get':
            try:
                machine_num = self.get_machine_num(self.host)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.leaderMachine, self.port))
                    update_message = {
                        'command': command.upper(),
                        'node_num': machine_num,
                        # 'file_name': file_name,
                        'sdfs_file_name': sdfs_file_name
                    }                    

                    s.sendall(json.dumps(update_message).encode('utf-8'))

            except Exception as e:
                print(str(e))           

        elif command == 'delete':
            if sdfs_file_name not in self.file_tracker.file_versions:
                print('this file is not in the system currently')
                return
            else:
                self.first_time_delete = False              
                folder_path = os.path.join(self.sdfsFolder, f"{sdfs_file_name}-*")
                files_to_delete = glob.glob(folder_path)
                if files_to_delete:
                    for file_to_delete in files_to_delete:
                        os.remove(file_to_delete)

                del self.file_tracker.file_versions[sdfs_file_name] # have to find all filenames that contains 
            try:
                machine_num = self.get_machine_num(self.host)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.leaderMachine, self.port))
                    update_message = {
                        'command': command.upper(),
                        'node_num': machine_num,
                        'sdfs_file_name': sdfs_file_name
                    }           

                    s.sendall(json.dumps(update_message).encode('utf-8'))

            except Exception as e:
                print(str(e))
    
    def find_machines_with_file(self, file_name, tracker, failed_host_num):
        keys_containing_element = []

        # Iterate through the dictionary
        for node, file_set in tracker.items():
            if node == failed_host_num:
                continue

            if file_name in file_set:
                keys_containing_element.append(node)

        return keys_containing_element

    def list_machines_with_file(self, sdfs_file_name):
        result = []
        for key, value in self.file_tracker.file_node.items():
            if sdfs_file_name in value:
                result.append(f'fa23-cs425-290{key}.cs.illinois.edu') if key != 10 else result.append(f'fa23-cs425-2910.cs.illinois.edu')
        
        return result
    
    def multi_read(self, sdfs_file_name, reading_machine_list):
        #convert hosto name into 
        host_addr_list = []
        for num in reading_machine_list:
            for host in ALL_HOSTS:
                if str(num) == host[14]:
                    host_addr_list.append(host)

        for host in host_addr_list:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((host, self.port))
                    msg = {
                        'status': 'multi_get',
                        'sdfs_file_name': sdfs_file_name
                    }

                    s.sendall(json.dumps(msg).encode('utf-8'))
            
            except Exception as e:
                print(str(e))

    def user_input(self):
        while True:
            if self.enable_sending:
                arg = input('Enter command: ')
                args = arg.split(' ')
                if args[0] == 'put':
                    #args[1] is the local filename, args[2] is sdfs filename
                    self.send_to_leader('put', args[1], args[2])
                elif args[0] == 'get':
                    #args[1] is the local filename, args[2] is sdfs filename
                    self.send_to_leader('get', args[1], args[2])
                elif args[0] == 'delete':
                    #args[1] is the sdfs filename
                    self.send_to_leader('delete', "", args[1])
                elif args[0] == 'join':
                    super().inputIsJoin()
                elif args[0] == 'ls':
                    print(self.list_machines_with_file(args[1]))
                elif args[0] == 'store':
                    machine_id = self.get_machine_num(self.host)
                    print(self.file_tracker.file_node[int(machine_id)])
                elif args[0] == 'multiread':
                    self.multi_read(args[1], args[2])
                elif args[0] == 'list_mem':
                    print(super().printMembershipList())
                elif args[0] == 'pt':
                    print('file-node', self.file_tracker.file_node)
                    print('file-versions', self.file_tracker.file_versions)
                elif args[0] == 'ml':
                    print(self.latest_failed_members)
                    print(self.leaderMachine)                
                else:
                    print('Error, input argument is invalid')
    
    def run(self):
        if self.host == self.leaderMachine:
            print('this is the leader machine')

        thread_failure_sender = threading.Thread(target = self.send_fd)
        thread_input = threading.Thread(target=self.user_input)
        thread_receiver = threading.Thread(target=self.receiver)
        thread_failure_receiver = threading.Thread(target=self.receive_fd)
        thread_rereplicate = threading.Thread(target=self.rereplicate)
        thread_elect_leader = threading.Thread(target=self.elect_leader)
        thread_queue = threading.Thread(target=self.queue_logic)
        thread_starvation = threading.Thread(target=self.Starvation)
        
        thread_failure_sender.start()
        thread_input.start()
        thread_receiver.start()
        thread_failure_receiver.start()
        thread_rereplicate.start()
        thread_elect_leader.start()
        thread_queue.start()
        thread_starvation.start()

        thread_failure_sender.join()
        thread_input.join()
        thread_receiver.join()
        thread_failure_receiver.join()
        thread_rereplicate.join()
        thread_elect_leader.join()
        thread_queue.join()
        thread_starvation.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--protocol-period', type=float, help='Protocol period T in seconds', default=0.25)
    parser.add_argument('-d', '--drop-rate', type=float,
                        help='The message drop rate',
                        default=0)
    args = parser.parse_args()

    log_file_path = "output.log"

    if not os.path.exists(log_file_path):
        # If it doesn't exist, create the log file
        with open(log_file_path, 'a') as log_file:
            pass
    else: 
        os.remove(log_file_path)

    log_file_path2 = "queue.log"

    if not os.path.exists(log_file_path2):
        # If it doesn't exist, create the log file
        with open(log_file_path2, 'a') as log_file:
            pass
    else: 
        os.remove(log_file_path2)

    def remove_all_files_in_folder(folder_path):
        # Check if the specified folder exists
        if not os.path.exists(folder_path):
            print(f"Folder '{folder_path}' does not exist.")
            return

        # List all files in the folder
        files = os.listdir(folder_path)

        # Remove each file in the folder
        for file in files:
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                except Exception as e:
                    print(f"Error while removing file: {file_path}\n{e}")

    # Example usage:
    folder_to_clear = "./sdfs"  # Replace with the folder path you want to clear

    remove_all_files_in_folder(folder_to_clear)

    test = Server(args)
    test.run()
    test.run_failure_detector()
