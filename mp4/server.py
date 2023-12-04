from fail_detector import Fail_Dector_Server
from fd import FailDetector
from fileserver import FileServer
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
import re
import ast
from collections import Counter, deque

'''
Meghan
maple maple_interconn.py 5 pref MP3_FILE -reg Interconne -matchAttrib Fiber
juice juice_interconn.py 5 pref result.txt 0 -hash hash

----------MAPLE FILTER COMMANDS
maple maple_filter.py 5 filtered MP3_FILE -reg Meghan -matchAttrib None


maple maple_filter.py 3 filtered input_files_directory -reg Melissa 
(17 results)

maple maple_filter.py 9 colin_z MP3_FILE -reg Meghan
maple maple_filter.py 9 colin_z MP3_FILE -reg None
maple maple_filter.py 6 colin_z MP3_FILE -reg Meghan

maple maple_filter.py 9 map1_fil input_files_directory -reg Meghan
x
 (3017 results) 

----------MAPLE JOIN COMMAND

maple maple_join.py 3 joined input_files_directory
maple maple_join.py 5 joined MP3_FILE

maple maple_join.py 9 joined MP3_FILE


----------JUICE FILTER COMMAND WITH HASH PARTITIONING

juice juice_filter.py 3 filtered result.txt 0 -hash hash
juice juice_filter.py 5 filtered result.txt 1 -hash hash
juice juice_filter.py 6 colin_z result.txt 1 -hash hash
juice juice_filter.py 9 colin_z result.txt 0 -hash hash

juice juice_filter.py 9 filtered result.txt 0 -hash hash

----------JUICE FILTER COMMAND WITH RANGE PARTITIONING

juice juice_filter.py 3 filtered result.txt 0 -range range
juice juice_filter.py 9 filtered result.txt 0 -range range
----------JUICE JOIN COMMAND WITH HASH PARTITIONING

juice juice_join.py 3 joined result222.txt 0 -hash hash
juice juice_join.py 5 joined result222.txt 0 -hash hash
juice juice_join.py 9 joined result222.txt 0 -hash hash


----------JUICE JOIN COMMAND WITH RANGE PARTITIONING

juice juice_join.py 3 joined result.txt 0 -range range
juice juice_join.py 9 joined result222.txt 0 -range range

juice juice_join.py 9 joined1 result22.txt 0 -range range

----------JUICE JOIN COMMAND WITH RANGE PARTITIONING

'''

'''
#SELECT * FROM employees.txt WHERE Meghan (works on 10) 3017 matches (range, hash)
#SELECT ALL FROM employees.txt departments.txt WHERE join_value = 48, (works on 10) 3915 matches (range, hash)
'''

NAME = "keyangx3"
# NAME = "colinz2"
SDFS_PATH = "MP3_FILE"
# SDFS_PATH = "MAP_LOCAL"
DEFAULT_PORT_NUM = 55549
DEFAULT_TASK_NUM = 9
LEADER_MACHINE = 'fa23-cs425-2901.cs.illinois.edu'
FINSHED_MAP_FILES_PATH = "finished_map_files"

#########hard code area
server_nums = [i for i in range(1, 11)]
host_name = 'fa23-cs425-29{}.cs.illinois.edu'
machine_2_ip = {i: 'fa23-cs425-29{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}  #host domain names of machnine 1~9
machine_2_ip[10] = 'fa23-cs425-2910.cs.illinois.edu'                                            #host domain name of machine 10
msg_format = 'utf-8'                #data encoding format of socket programming
filelocation_list = {} # sdfs_filename: [ips which have this file]
host_domain_name = socket.gethostname() 
machine_id = int(host_domain_name[13:15])
file_reciever_port = 54321
file_leader_port = 5009
file_sockets = {}
leader_queue = list()
mp3_log_path = f'/home/{NAME}/mp4/MP3_log'
#########
WORKER_HOSTS = [
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

class Server(FileServer,FailDetector):
    def __init__(self):
        super().__init__()
        self.host = socket.gethostname()
        self.port = DEFAULT_PORT_NUM
        self.task_num = DEFAULT_TASK_NUM
        self.leaderMachine = LEADER_MACHINE
        self.host_domain_name = socket.gethostname() 
        self.progress_tracker = {self.get_machine_num(host): 'IDLE' for host in WORKER_HOSTS} # to track the progress of the tasks 
        self.queue = queue.Queue(maxsize=10)  # Queue to handle incoming requests
        self.regex = ""
        self.join_value = "48"
        self.querier_Machine = None #machine that sent initial query 
        self.result_file_path = None
        self.task_num = 9
        self.delete_intermed_file = None
        self.fail_but_not_process = [] # failed vm that is waiting for its replication to finish
        self.gossiping_recv_port = 4999
        self.gossiping_sockets = {}   #record all socket for gossiping
        self.membership_list = {} #dict() dict([domain_name, heartbeat, localtime, status])
        self.gossiping_threadpool = {}
        self.machine_2_ip = {i: 'fa23-cs425-29{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}  #host domain names of machnine 1~9
        self.machine_2_ip[10] = 'fa23-cs425-2910.cs.illinois.edu'  
        self.introducer_port = 5004
        self.memberlist_lock = threading.Lock()
        self.heartbeatrate = 0.07
        self.status_join = set()
        self.gossiping_timeout = 0.1
        self.sock_timeout = 0.08
        self.recieve_time = 0
        self.msg_drop_rate = 0.0
        self.failure_queue = deque()
        self.filelocation_intro_queue = deque()
        self.failure_time = 0
        self.T_fail = 4
        self.T_cleanup = 2
        self.intermed_prefix = None
        self.col_index = None
        self.match_value = None
        self.total_num_rows_matching_match_value = 0
        self.maple_worker_index = []
        # self.lock = True

    def get_machine_num(self, host): #get the machine number
        return int(host[14]) if host[14] != '0' else 10

    def check_num_worker(self):
        while True:
            num = len(self.membership_list)
    
    def get_machine_ip(self, node_num): # get machine ip
        return f'fa23-cs425-290{node_num}.cs.illinois.edu' if node_num != 10 else f'fa23-cs425-2910.cs.illinois.edu'
    
    def extract_numeric_part(self, file_name): # for sorting the partition directory if there is a join
    # Extracts the numeric part from the file name
        split_file_name = file_name.split('_')
        match = re.search(r'\d+', file_name)
        return int(match.group()) if match and self.intermed_prefix == split_file_name[0] else float('inf')

    def queue_logic(self): #logic for the queue
        count = 1
        while True:
            # while self.lock:
                if self.host == self.leaderMachine:
                    time.sleep(1)
                    parallel_execution = []
            
                    data = self.queue.get() #remove item from queue

                    if data["job_type"] == "juice":
                        print('inside juice clause in queue')

                        self.result_file_path = data["output_file"]
                        self.delete_intermed_file = data["delete_input"]
                        self.intermed_prefix = data["intermed_prefix"]

                        partition_type = data["partition_type"]

                        if partition_type.strip() == "hash":
                            self.hash_partition_file_juice(data["intermed_prefix"], data["executable"])
                        else:
                            self.range_partition_file_juice(data["intermed_prefix"], data["executable"])

                    elif data["command"] == "FILTER" or data["command"] == None:
                        print('data command filter in queue')
                        self.intermed_prefix = data["intermed_prefix"]
                        # self.lock = False
                        # print(self.lock)
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                if data['node_num'] != 10:
                                    host = f"fa23-cs425-290{data['node_num']}.cs.illinois.edu"
                                else:
                                    host = f"fa23-cs425-2910.cs.illinois.edu"
                                s.connect((host, self.port))
                                if data["job_type"] == "maple":
                                    leader_message = {
                                        'ack_command': data['command'],
                                        'node_num': data['node_num'],
                                        'executable': data['executable'],
                                        'job_type': data["job_type"],
                                        'task_num': data["task_num"],
                                        'intermed_prefix': data['intermed_prefix'],
                                        'output_file': data["output_file"],
                                        'delete_input': data["delete_input"],                                    
                                        'partition_type': data["partition_type"],
                                        'regex': data['regex'],
                                        'ack_file': data['file_names'],
                                        'match_attrib': data['match_attrib']
                                    }
                                s.sendall(json.dumps(leader_message).encode('utf-8'))
                                print('Filter allowed')
                        except Exception as e:
                            print(str(e))

                    elif data["command"] == "JOIN":
                        self.lock = False
                        self.intermed_prefix = data["intermed_prefix"]
                    
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                if data['node_num'] != 10:
                                    host = f"fa23-cs425-290{data['node_num']}.cs.illinois.edu"
                                else:
                                    host = f"fa23-cs425-2910.cs.illinois.edu"
                                s.connect((host, self.port))
                                leader_message = {
                                    'ack_command': data['command'],
                                    'node_num': data['node_num'],
                                    'executable': data['executable'],
                                    'job_type': data["job_type"],
                                    'task_num': data["task_num"],
                                    'intermed_prefix': data['intermed_prefix'],
                                    'output_file': data["output_file"],
                                    'delete_input': data["delete_input"],                                    
                                    'ack_file': [data['file_names'][0], data['file_names'][1]],
                                    'partition_type': data["partition_type"],
                                    'regex': data['regex'],
                                    'match_attrib': data['match_attrib']                                    
                                    # 'ack_command': 'JOIN',
                                    # 'node_num': data['node_num'],
                                    # 'ack_file': [data['file_names'][0], data['file_names'][1]]
                                }

                                

                                s.sendall(json.dumps(leader_message).encode('utf-8'))
                                print('Join allowed')

                        except Exception as e:
                            print(str(e))
    
    def send_job_to_leader(self, job_type, executable, numTasks, prefix, input_directory, output_file, delete_input, regex, partitionType, matchAttrib):
        print(matchAttrib)
        node_num = self.get_machine_num(self.host)
        print(input_directory)
        input_files = [file for file in os.listdir(input_directory) if "input_file" in file]
        sql_operation = None

        if delete_input is not None:
            delete_input = int(delete_input)

        if "join" in executable:
            regex = self.join_value

        if "filter" in executable: #figure out which executable to run
            sql_operation = "FILTER"
        elif "join" in executable:
            sql_operation = "JOIN"
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.leaderMachine, self.port))
                print('connected')
                update_message = {
                    'command': sql_operation,
                    'node_num': node_num,
                    'executable': executable,
                    'job_type': job_type,
                    'task_num': int(numTasks),
                    'intermed_prefix': prefix,
                    'file_names': input_files,
                    'output_file': output_file,
                    'delete_input': delete_input,
                    'regex': regex,
                    'partition_type': partitionType,
                    'match_attrib': matchAttrib
                }
           
                s.sendall(json.dumps(update_message).encode('utf-8'))
                print('sent message to leader')
        except Exception as e:
            print(str(e))

    def hash_partition_file(self, file_name, num_partitions, intermed_prefix, regex, match_attrib=None): #hash partition before maple stage

        partitions = {}
        print('list of file_name, ', file_name)

        for file_name in file_name:
            print('name of file dealing with, ', file_name)
            try:
                with open(f'MP3_FILE/{file_name}', 'r', encoding='utf-8-sig') as file:
                    for line in file:
                        # print(type(match_attrib))
                        # print(match_attrib)
                        if str(match_attrib) != 'None' and self.match_value is None:
                            first_line = line.strip()
                            split_first_line = first_line.split(',')
                            self.col_index = split_first_line.index(regex) #find the index of the column name
                            self.match_value = match_attrib #the specific value in that column that we want to extract
                        elif self.match_value is not None and self.col_index is not None:
                            line = line.strip()
                            split_first_line = line.split(',')
                            if split_first_line[self.col_index] == match_attrib:
                                self.total_num_rows_matching_match_value += 1

                        partition_key = hash(line) % num_partitions
                        # Create a partition if it doesn't exist
                        if partition_key not in partitions:
                            partitions[partition_key] = []
                        # Append the line to the corresponding partition
                        partitions[partition_key].append(line.strip())
                    
                    print('total num rows matching. ', self.total_num_rows_matching_match_value)

            except FileNotFoundError:
                print(f"Error: File '{file_name}' not found.")
                return None

            name_without_extension = (file_name.split('.')[0]).split('/')[-1]

            # for partition_key, lines in partitions.items():
            #     if partition_key != 0:
            #         partition_name =  os.path.join(SDFS_PATH, f'{partition_key}_{intermed_prefix}_partition.txt')
            #     else:
            #         partition_name =  os.path.join(SDFS_PATH, f'{10}_{intermed_prefix}_partition.txt')

            #     with open(partition_name, 'w') as partition_file:
            #         partition_file.write('\n'.join(lines))

            lines_list = []
            for _, lines in partitions.items():
                lines_list.append(lines)



            print(len(lines_list))
            for _, host in enumerate(list(self.membership_list.keys())):
                if host != 'fa23-cs425-2901.cs.illinois.edu':
                    key = int(host[14]) if host[14] != '0' else 10
                    partition_name =  os.path.join(SDFS_PATH, f'{key}_{intermed_prefix}_partition.txt')
                    self.maple_worker_index.append(key)

                    with open(partition_name, 'w') as partition_file:
                        partition_file.write('\n'.join(lines_list[0]))
                    lines_list.pop(0)


    def find_file_without_number(self, directory): #map_department_joined_result
        matching_files = [file_name for file_name in directory if file_name.split("_")[-1].isdigit() == False and "joined_result" in file_name]
        return matching_files
    
    def hash_partition_file_juice(self, intermed_prefix, executable): #hash partition before juice stage
        print('inside juice hash partition')
        partitions = {}
        directory = os.listdir(SDFS_PATH)
        prefix = None
        is_operation_join = None
        join_file = self.find_file_without_number(directory) #find the file from department table that matches (should be only one!)
        
        print("join_file ", join_file)
        print('intermed_prefix, ', intermed_prefix)

        if len(join_file) == 0:
            is_operation_join = False
        elif len(join_file) > 0 and intermed_prefix not in join_file[0]:
            is_operation_join = False
        else:
            is_operation_join = True

        for file in directory:
            if len(join_file) > 0 and join_file[0] == file: continue #if the file doesn't end with a number, that is department file, so ignore

            if intermed_prefix in file and "map" in file and "result" in file:
                print(file)
                try:
                    with open(os.path.join(SDFS_PATH, file), 'r') as result_map_file:
                        for line in result_map_file:
                            partition_key = hash(line) % (len(self.membership_list) - 1)
                            # partition_key = hash(line) % len(WORKER_HOSTS)
                            # Create a partition if it doesn't exist
                            if partition_key not in partitions:
                                partitions[partition_key] = []
                            # Append the line to the corresponding partition
                            partitions[partition_key].append(line.strip())

                except FileNotFoundError:
                    print(f"Error: File '{file}' not found.")
                    return None  
        


        lines_list = []
        for _, lines in partitions.items():
            lines_list.append(lines)
        
        non_used_key = []
        for _, host in enumerate(list(self.membership_list.keys())):
            if host != 'fa23-cs425-2901.cs.illinois.edu':
                key = int(host[14]) if host[14] != '0' else 10
                non_used_key.append(key)

        print(len(lines_list))
        for _, host in enumerate(list(self.membership_list.keys())):
            if host != 'fa23-cs425-2901.cs.illinois.edu':
                key = int(host[14]) if host[14] != '0' else 10
                partition_name =  os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{key}")
                non_used_key.remove(key)
                if is_operation_join == False:
                    with open(partition_name, 'a') as partition_file:
                        partition_file.write('\n'.join(lines_list[0]) + '\n')    
                    lines_list.pop(0)
                    if (len(lines_list)) == 0:
                        break
                else:
                    join_file_path = os.path.join(SDFS_PATH, join_file[0])
                    with open(partition_name, 'a') as partition_file, open(join_file_path) as joinFile:
                        join_line = joinFile.readline() #grab the first line

                        for line in lines:
                            partition_file.write(f'{line.strip()},{join_line}')

        print(non_used_key)

        if non_used_key != []:
            for key in non_used_key:
                partition_name =  os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{key}")
                if is_operation_join == False:
                    with open(partition_name, 'a') as partition_file:
                        pass

        self.send_juice_partition_worker(is_operation_join, intermed_prefix, executable)                
        # for partition_key, lines in partitions.items():
        #     if partition_key != 0:
        #         partition_name =  os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{partition_key + 1}")
        #     else:
        #         partition_name =  os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{10}") # needs to be changed!!!!!! to 10

        #     if is_operation_join == False:
        #         with open(partition_name, 'a') as partition_file:
        #             partition_file.write('\n'.join(lines) + '\n')            
        #     else:
        #         join_file_path = os.path.join(SDFS_PATH, join_file[0])
        #         with open(partition_name, 'a') as partition_file, open(join_file_path) as joinFile:
        #             join_line = joinFile.readline() #grab the first line

        #             for line in lines:
        #                 partition_file.write(f'{line.strip()},{join_line}')


    
    def range_partition_file_juice(self, intermed_prefix, executable): #range partitioning for juice stage
        print('in juice range partition')
        directory = os.listdir(SDFS_PATH)
        is_operation_join = None
        join_file = self.find_file_without_number(directory) #find the file from department table that matches (should be only one!)

        print('length of join file, ', len(join_file))
        print('intermed_prefix, ', intermed_prefix)

        if len(join_file) == 0:
            is_operation_join = False
        elif len(join_file) > 0 and intermed_prefix not in join_file[0]:
            is_operation_join = False
        else:
            is_operation_join = True

        for file in directory:
            if len(join_file) > 0 and join_file[0] == file: continue
            if intermed_prefix in file and "map" in file and "result" in file:
                # print('filename in range is, ', file)
                with open(os.path.join(SDFS_PATH, file), 'r') as result_map_file:
                    for line in result_map_file:
                        print('line is, ', line)
                        if "department" in line or "employee" in line: continue
                        k_v = ast.literal_eval(line.strip())
                        partition_key = int(k_v[0]) % len(WORKER_HOSTS)

                        if is_operation_join == False:
                            if partition_key != 0:
                                with open(os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{partition_key + 1}"), 'a') as juiceFile:
                                    juiceFile.write(line)
                            
                            else:
                                with open(os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{10}"), 'a') as juiceFile: #!!!not supposed to be 4, supposd to be 10!
                                    juiceFile.write(line)
                        else:
                            join_file_path = os.path.join(SDFS_PATH, join_file[0])
                            if partition_key != 0:
                                with open(os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{partition_key + 1}"), 'a') as juiceFile, open(join_file_path, 'r') as joinFile:
                                    join_line = joinFile.readline() #grab the first line
                                    juiceFile.write(f'{line.strip()},{join_line}')
                            
                            else:
                                with open(os.path.join(SDFS_PATH, f"juice_task_{intermed_prefix}_{10}"), 'a') as juiceFile, open(join_file_path, 'r') as joinFile: #!!!not supposed to be 4, supposd to be 10!
                                    join_line = joinFile.readline() #grab the first line
                                    juiceFile.write(f'{line.strip()},{join_line}')
        
        self.send_juice_partition_worker(is_operation_join, intermed_prefix, executable)
        print('ALL JUICE TASKS HAVE BEEN SENT BY LEADER')    

    def send_juice_partition_worker(self, operation, intermed_prefix, executable):
        directory = os.listdir(SDFS_PATH)
        task_type = None
        print('operation in send juice partition worker, ', operation)

        if operation == False:
            task_type = "FILTER"
        else:
            task_type = "JOIN"
        
        if task_type == "FILTER":
            for file in directory:
                if f"juice_task_{intermed_prefix}" in file:
                    key = int(file.split("_")[-1])

                    if key != 10: 
                        prefix = f'{NAME}@fa23-cs425-290{key}.cs.illinois.edu:/home/{NAME}/mp4/{SDFS_PATH}'
                        send_to_prefix = f'fa23-cs425-290{key}.cs.illinois.edu'

                    else: 
                        prefix = f'{NAME}@fa23-cs425-2910.cs.illinois.edu:/home/{NAME}/mp4/{SDFS_PATH}'
                        send_to_prefix = 'fa23-cs425-2910.cs.illinois.edu'

                    p = subprocess.Popen(['scp', os.path.join(SDFS_PATH, file), prefix])
                    code = p.wait()

                    if code == 0:
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as juice_socket:
                                juice_socket.connect((send_to_prefix, self.port))
                                map_task_msg = {
                                    'command': 'JUICE-TASK',
                                    'task_type': task_type,
                                    'executable': executable,
                                    'file_name': file,
                                    'intermed_prefix': intermed_prefix,
                                    'total_rows_match_col': self.total_num_rows_matching_match_value
                                }

                                juice_socket.sendall(json.dumps(map_task_msg).encode('utf-8'))
                                print('juice task sent to worker node')
                                self.progress_tracker[partition_key] = 'JUICE'
                        except Exception as e:
                            print(str(e))                    
        else:
            for file in directory:
                if "juice" in file and "join" in file:
                    key = int(file.split("_")[-1])
                    if key != 10: 
                        prefix = f'{NAME}@fa23-cs425-290{key}.cs.illinois.edu:/home/{NAME}/mp4/{SDFS_PATH}'
                        send_to_prefix = f'fa23-cs425-290{key}.cs.illinois.edu'

                    else: 
                        prefix = f'{NAME}@fa23-cs425-2910.cs.illinois.edu:/home/{NAME}/mp4/{SDFS_PATH}'
                        send_to_prefix = 'fa23-cs425-2910.cs.illinois.edu'

                    p = subprocess.Popen(['scp', os.path.join(SDFS_PATH, file), prefix])
                    code = p.wait()

                    if code == 0:
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as juice_socket:
                                juice_socket.connect((send_to_prefix, self.port))
                                map_task_msg = {
                                    'command': 'JUICE-TASK',
                                    'task_type': task_type,
                                    'executable': executable,
                                    'file_name': file,
                                    'intermed_prefix': intermed_prefix,
                                    'total_rows_match_col': 0
                                }

                                juice_socket.sendall(json.dumps(map_task_msg).encode('utf-8'))
                                print('juice task sent to all worker nodes')
                                self.progress_tracker[partition_key] = 'JUICE'
                        except Exception as e:
                            print(str(e))

        print('ALL JUICE TASKS HAVE BEEN SENT BY LEADER')
    
    def send_result_to_querier(self):
        directory_path = SDFS_PATH
        combined_file_path = self.result_file_path

        with open(combined_file_path, 'w') as combined_file:
            # Iterate over files in the folder
            for filename in os.listdir(directory_path):
                if f'result_{self.intermed_prefix}' in filename:
                    file_path = os.path.join(directory_path, filename)

                    # Check if the item in the folder is a file (not a directory)
                    if os.path.isfile(file_path):
                        # Open each file and read its contents
                        with open(file_path, 'r') as current_file:
                            # Read the contents of the file
                            file_contents = current_file.read()
                            # Write the contents to the combined file
                            combined_file.write(file_contents)
        
        prefix = f'{NAME}@{self.querier_Machine}:/home/{NAME}/mp4/{SDFS_PATH}/'
        p = subprocess.Popen(['scp', combined_file_path, prefix])
        code = p.wait()

        if code == 0:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as send_socket:
                    send_socket.connect((self.querier_Machine, self.port))
                    done_msg = {
                        'command': 'MAPLE-JUICE-DONE',
                    }

                    send_socket.sendall(json.dumps(done_msg).encode('utf-8'))
                    print('MAPLE JUICE OPERATION SUCCESSFUL')
            except Exception as e:
                print(str(e))

        self.col_index = None
        self.match_value = None
        
        if self.delete_intermed_file == 1: #delete all intermed files
            print('SEND DELETE INTERMEDIATE FILE MESSAGE TO ALL WORKERS')
            if self.host == self.leaderMachine:
                self.clear_folder(SDFS_PATH)
            for worker in WORKER_HOSTS:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as delete_file_socket:
                        delete_file_socket.connect((worker, self.port))
                        done_msg = {
                            'command': 'DELETE_INTERMEDIATE_FILES',
                            'intermed_prefix': self.intermed_prefix
                        }

                        delete_file_socket.sendall(json.dumps(done_msg).encode('utf-8'))
                except Exception as e:
                    print(str(e))       

    def clear_folder(self, folder_path):
        try:
            # List all files in the folder
            files = os.listdir(folder_path)

            # Iterate through each file and remove it
            for file_name in files:
                file_path = os.path.join(folder_path, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                else:
                    # If it's a directory, remove it recursively
                    os.rmdir(file_path)

            print(f"Folder '{folder_path}' cleared successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")   

    def get_map_partitions_prefix(self, directory, intermed_prefix):
        matching_map_partitions_paths = []
        partition_files = os.listdir(directory)

        for file in partition_files:
            print('files, ', file)
            if intermed_prefix in file and "partition" in file:
                matching_map_partitions_paths.append(file)
        
        return matching_map_partitions_paths

        
    def send_map_partitions_worker(self, regex_to_match, operation, intermed_prefix, executable):
        directory_path = "MP3_FILE/"

        matching_paths = self.get_map_partitions_prefix(directory_path, intermed_prefix)
        
        print("matching_paths, ", matching_paths)

        if operation == "FILTER" or operation == None:
            for _, file in enumerate(matching_paths):
                index = int(file.split('_')[0])
                index = index - 2
                print('index in send map partition is, ', index)
                path = os.path.join(SDFS_PATH, file)
                prefix = f"{NAME}@{WORKER_HOSTS[index]}:/home/{NAME}/mp4/MP3_FILE"
                super().send2Leader("put_worker", file, file)

                p = subprocess.Popen(['scp', path, prefix])
                code = p.wait()

                if code == 0:
                    node_num = self.get_machine_num(WORKER_HOSTS[index])
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as workSocket:
                        workSocket.connect((WORKER_HOSTS[index], self.port))
                        map_task_msg = {
                            'command': 'MAPLE-TASK',
                            'task_type': 'FILTER',
                            'node_num': node_num,
                            'executable': executable,
                            'file_name': file,
                            'regex': regex_to_match,
                            'intermed_prefix': intermed_prefix,
                            'match_attrib': self.match_value,
                            'col_value': self.col_index
                        }
                        workSocket.sendall(json.dumps(map_task_msg).encode('utf-8'))
                        print('MAPLE task sent to all worker nodes')
                        self.progress_tracker[node_num] = 'MAPLE'       

        elif operation == "JOIN": #for JOIN
            temp = matching_paths
            sorted_partition_list = sorted(temp, key=self.extract_numeric_part) #sorted partition 
            print('sorted_partition_list, ', sorted_partition_list)

            for index in range(len(sorted_partition_list)):
                if intermed_prefix in sorted_partition_list[index]:
                    key = int(sorted_partition_list[index].split('_')[0])
                    key = key - 2
                    item_path = os.path.join(directory_path, sorted_partition_list[index])
                    prefix = f"{NAME}@{WORKER_HOSTS[key]}:/home/{NAME}/mp4/MP3_FILE"
                    p = subprocess.Popen(['scp', item_path, prefix])
                    code = p.wait()
          
                    if code == 0:
                        node_num = self.get_machine_num(WORKER_HOSTS[key])
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as workSocket:
                            print(WORKER_HOSTS[key])
                            workSocket.connect((WORKER_HOSTS[key], self.port))
                            map_task_msg = {
                                'command': 'MAPLE-TASK',
                                'task_type': 'JOIN',
                                'node_num': node_num,
                                'executable': executable,
                                'file_name': [sorted_partition_list[index]],
                                'regex': regex_to_match,
                                'intermed_prefix': intermed_prefix,
                                'col_value': None
                            }
                            workSocket.sendall(json.dumps(map_task_msg).encode('utf-8'))
                            print('MAPLE task sent to all worker nodes')
                            self.progress_tracker[node_num] = 'MAPLE'

    def all_done(self):
     
        survive_list_index = []
        for _, host in enumerate(list(self.membership_list.keys())):
            if host != 'fa23-cs425-2901.cs.illinois.edu':
                key = int(host[14]) if host[14] != '0' else 10
                key = key - 2
                survive_list_index.append(key)

        return survive_list_index

    def monitor_progress_tracker(self):
        while True:
            survive_list = self.all_done()
   
            progress_tracker = list(self.progress_tracker.values()) 
            if all(progress_tracker[i] == "MAPLE-DONE" for i in survive_list) and survive_list != []:
            # if all(value == 'MAPLE-DONE' for value in self.progress_tracker.values()):
                self.progress_tracker = {key: 'IDLE' for key in self.progress_tracker}
                print('MAPLE TASKS HAVE ALL BEEN FINISHED')

            if all(progress_tracker[i] == "JUICE-DONE" for i in survive_list) and survive_list != []:
            # elif all(value == 'JUICE-DONE' for key, value in self.progress_tracker.items() if key != 1):
                self.progress_tracker = {key: 'IDLE' for key in self.progress_tracker}
                self.send_result_to_querier()
            
    def receiver(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen(1)
            while True:
                conn, addr = s.accept()
                payload = conn.recv(4096)
                if not payload:
                    print("HERE")
                    break
                data = json.loads(payload.decode('utf-8'))

                if self.host == self.leaderMachine:
                    print("RECEIVE")
                    if "command" in data:
                        # self.task_num = data['task_num']
                        if data["command"] == "FILTER" or data["command"] == "JOIN" or data["command"] == None:
                            
                            if len(self.membership_list) - 1 >= data['task_num']:
                                self.querier_Machine = self.get_machine_ip(data["node_num"])
                                self.queue.put(data)
                            else: 
                                data['task_num'] = len(self.membership_list)-1
                                self.querier_Machine = self.get_machine_ip(data["node_num"])
                                self.queue.put(data)

                        elif data["command"] == "MAPLE-DONE":
                            print('received MAPLE is done')
                            self.progress_tracker[data["node_num"]] = 'MAPLE-DONE'
                        
                        elif data["command"] == "JUICE-DONE":
                            print('RECEIVED JUICE DONE MESSAGE')
                            self.progress_tracker[data["node_num"]] = 'JUICE-DONE'

                    elif "scp" in data:
                        print("Recieve the message that scp has done")
                        self.hash_partition_file(data['file_name'], data['task_num'], data['intermed_prefix'], data['regex'], match_attrib=data['match_attrib'])
                        self.send_map_partitions_worker(data["regex"], data["operation"], data["intermed_prefix"], data['executable'])
                    # self.lock = True
                        # print(self.lock)
                else:
                    if "command" not in data:
                        if 'ack_file' in data: #for processing maple stage (filter and join)
                            print("Receive the acknowledgement from leader, please do scp:")
                            for file_name in data['ack_file']:
                                prefix = f"{NAME}@{self.leaderMachine}:/home/{NAME}/mp4"
                                prefix = f"{NAME}@{self.leaderMachine}:/home/{NAME}/mp4"

                                p = subprocess.Popen(['scp', f'MP3_FILE/{file_name}', prefix + '/' + os.path.join(SDFS_PATH, file_name)])

                                stdout, stderr = p.communicate()
                                # Check the return code of the process
                                return_code = p.returncode
                                if return_code == 0:
                                    continue
                            try:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
                                    s2.connect((self.leaderMachine, self.port))
                                    print(data['ack_command'])
                                    if data['ack_command'] == "FILTER" or data['ack_command'] == "JOIN" or data['ack_command'] == None:
                                        update_message = {
                                            'operation': data['ack_command'],
                                            'node_num': data['node_num'],
                                            'executable': data['executable'],
                                            'job_type': data["job_type"],
                                            'task_num': data['task_num'],
                                            'intermed_prefix': data['intermed_prefix'],
                                            'output_file': data["output_file"],
                                            'delete_input': data["delete_input"],                                    
                                            'file_name': data['ack_file'],
                                            'regex': data['regex'],
                                            'partition_type': data['partition_type'],
                                            'match_attrib': data['match_attrib'],
                                            'scp': 'DONE'
                                        }
                                    s2.sendall(json.dumps(update_message).encode('utf-8'))
                                    
                            except Exception as e:
                                print(str(e))
                
                            print('SCP DONE, already sent partitioning signal to leader')

                    elif data["command"] == "MAPLE-TASK":
                        print('MAPLE task data, ', data)
                        node_num = self.get_machine_num(self.host)
                        intermed_prefix = data["intermed_prefix"]

                        if isinstance(data["file_name"], list):
                            data["file_name"] = ', '.join(data["file_name"])

                        try:
                            # Specify the command to run script2.py
                            if data["col_value"] is not None:
                                print('col value is, ', data["col_value"])
                                print('match_attrib is, ', data["match_attrib"])

                                command = ["python3", data["executable"], data['file_name'], str(data["match_attrib"]), str(data['col_value']), intermed_prefix, str(node_num)]
                                # Use subprocess.Popen to start the process
                                process = subprocess.Popen(command)
                                # Wait for the process to finish and get the return code
                                return_code = process.wait()
                            else:
                                command = ["python3", data["executable"], data['file_name'], data['regex'], str(node_num), intermed_prefix]
                                # Use subprocess.Popen to start the process
                                process = subprocess.Popen(command)
                                # Wait for the process to finish and get the return code
                                return_code = process.wait()

                            print(f"finished with return code: {return_code}")

                        except subprocess.CalledProcessError as e:
                            print(f"Error running maple file: {e}")

                        print('done with map stage for join')
                            
                        prefix = f"{NAME}@{self.leaderMachine}:/home/{NAME}/mp4"
                        code = None

                        if os.path.exists(f"MP3_FILE/map_{intermed_prefix}_result_{node_num}"): #FOR FILTER AND NONE sql command
                            p = subprocess.Popen(['scp', os.path.join("MP3_FILE", f"map_{intermed_prefix}_result_{node_num}"), prefix + '/' + os.path.join(SDFS_PATH, f"map_{intermed_prefix}_result_{node_num}")])
                            code = p.wait()

                        if os.path.exists(f"MP3_FILE/map_employee_{intermed_prefix}_result_{node_num}"):
                            print('inside employee keys')
                            p = subprocess.Popen(['scp', os.path.join("MP3_FILE", f"map_employee_{intermed_prefix}_result_{node_num}"), prefix + '/' + os.path.join(SDFS_PATH, f"map_employee_{intermed_prefix}_result_{node_num}")])
                            code = p.wait()

                        if os.path.exists(f"MP3_FILE/map_department_{intermed_prefix}_result"):
                            print('inside department keys')
                            p = subprocess.Popen(['scp', os.path.join("MP3_FILE", f"map_department_{intermed_prefix}_result"), prefix + '/' + os.path.join(SDFS_PATH, f"map_department_{intermed_prefix}_result")])
                            code = p.wait()                            

                        if code == 0:
                            try:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mapDoneSocket:
                                    mapDoneSocket.connect((self.leaderMachine, self.port))
                                    update_message = {
                                        'command': 'MAPLE-DONE',
                                        'task_type': data["task_type"],
                                        'node_num': node_num
                                    }

                                    mapDoneSocket.sendall(json.dumps(update_message).encode('utf-8'))
                                    print(f'Node {node_num} is DONE with MAPLE task')
                            except Exception as e:
                                print(str(e))                         
                            print(f'MAPLE-TASK ON NODE {node_num} COMPLETED')
                    
                    elif data["command"] == "JUICE-TASK":
                        print('JUICE task data, ', data)
                        node_num = self.get_machine_num(self.host)
                        file_prefix = data["intermed_prefix"]

                        try:
                            # Specify the command to run script2.py
                            command = ["python3", data["executable"], data['file_name'], str(node_num), intermed_prefix]

                            if data["total_rows_match_col"] != 0:
                                command = ["python3", data["executable"], data['file_name'], str(node_num), intermed_prefix, str(data["total_rows_match_col"])]

                            # Use subprocess.Popen to start the process
                            process = subprocess.Popen(command)
                            # Wait for the process to finish and get the return code
                            return_code = process.wait()

                            print(f"finished with return code: {return_code}")

                        except subprocess.CalledProcessError as e:
                            print(f"Error running juice file: {e}")                        
                        
                        prefix = f"{NAME}@{self.leaderMachine}:/home/{NAME}/mp4/{SDFS_PATH}"
                        p = subprocess.Popen(['scp', f'{SDFS_PATH}/result_{file_prefix}_{node_num}', prefix])
                        code = p.wait()                        

                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as juiceDoneSocket:
                                juiceDoneSocket.connect((self.leaderMachine, self.port))

                                update_message = {
                                    'command': 'JUICE-DONE',
                                    'node_num': node_num
                                }

                                juiceDoneSocket.sendall(json.dumps(update_message).encode('utf-8'))
                                print(f'Node {node_num} is DONE with JUICE task')
                        except Exception as e:
                            print(str(e))                         
                        print(f'JUICE-TASK ON NODE {node_num} COMPLETED')

                    elif data["command"] == 'MAPLE-JUICE-DONE':
                        print("MAPLE JUICE OPERATION COMPLETED SUCCESSFULLY")
                    
                    elif data["command"] == "DELETE_INTERMEDIATE_FILES":
                        mp3_direct = os.listdir(SDFS_PATH)
                        delete_prefix = data["intermed_prefix"]

                        for file in mp3_direct:
                            if f'{delete_prefix}_' in file or f'{delete_prefix}_result' in file:
                                file_path = os.path.join(SDFS_PATH, file)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)

    def run(self):
        if self.host == self.leaderMachine:
            print('this is the leader machine')
            # thread_introducer = threading.Thread(target=introducer_run)

        thread_input = threading.Thread(target=self.user_input)
        thread_receiver = threading.Thread(target=self.receiver)
        thread_queue = threading.Thread(target=self.queue_logic)
        thread_progress = threading.Thread(target=self.monitor_progress_tracker)
        # thread_rereplicate = threading.Thread(target=self.rereplicate)
        thread_fd_send = threading.Thread(target=self.start_gossiping)
        


        # thread_introducer.start()
        thread_receiver.start()
        thread_input.start()
        thread_queue.start()
        thread_progress.start()
        # thread_rereplicate.start()
        thread_fd_send.start()

        # thread_introducer.join()
        thread_receiver.join()
        thread_input.join()
        thread_queue.join()
        thread_progress.join()
        # thread_rereplicate.join()
        thread_fd_send.join()

    def user_input(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-reg', '--reg', dest='regex', type=str, help='regex to filter for', default="")
        parser.add_argument('-hash', '--hash', dest='hash', type=str, help='do hash partition', default="")
        parser.add_argument('-range', '--range', dest='range', type=str, help='do range partition', default="")
        parser.add_argument('-matchAttrib', '--matchAttrib', dest='matchAttrib', type=str, help='csv column attribute', default="")

        # parser.add_argument('-join', '--join', dest='join', type=str, help='join attribute', default="")

        while True: #when testing, make sure that you input maple, numTasks, prefix in that order
            arg = input('Enter command: ')
            args = arg.split(' ')
            regex = None
            range_part = None
            hash_part = None
            matchAttrib_part = None

            try:
                reg_index = args.index('-reg') if '-reg' in args else args.index('--reg')
            except ValueError:
                reg_index = -1

            try:
                hash_index = args.index('-hash') if '-hash' in args else args.index('--hash')
                print('hash_index, ', hash_index)
            except ValueError:
                hash_index = -1

            try:
                range_index = args.index('-range') if '-range' in args else args.index('--range')
            except ValueError:
                range_index = -1

            try:
                matchAttrib_index = args.index('-matchAttrib') if '-matchAttrib' in args else args.index('--matchAttrib')
                print('matchattrib, ', matchAttrib_index)
            except ValueError:
                matchAttrib_index = -1                

            # Parse the arguments if the flag is found
            if reg_index != -1 and reg_index + 1 < len(args):
                regex = parser.parse_args(args[reg_index:reg_index + 2])
                print(regex)
            
            if hash_index != -1:
                hash_part = parser.parse_args(args[hash_index:hash_index + 2])
            
            if range_index != -1:
                range_part = parser.parse_args(args[range_index: range_index + 2])

            if matchAttrib_index != -1:
                print('in match attrib')
                matchAttrib_part = parser.parse_args(args[matchAttrib_index: matchAttrib_index + 2])                  


            if args[0] == 'maple':
                print('test')
                if regex is not None:
                    print('flag value is', regex.regex)
                    self.send_job_to_leader(args[0], args[1], args[2], args[3], args[4], None, None, regex.regex, None, matchAttrib_part.matchAttrib)
                else:
                    self.send_job_to_leader(args[0], args[1], args[2], args[3], args[4], None, None, None, None, None)
            elif args[0] == 'juice':
                partition_style = hash_part.hash if hash_part is not None else range_part.range
                self.send_job_to_leader(args[0], args[1], args[2], args[3], None, args[4], args[5], None, partition_style, None)
                print('juice')
            elif args[0] == 'ml':
                print(super().printMembershipList())
            elif args[0] == 'pt':
                print(self.progress_tracker)
            elif args[0] == 'lm':
                print(self.membership_list)
            else: 
                print('incorrect arguments, try again')

if __name__ == "__main__":

    # def clear_folder(folder_path):
    #     try:
    #         # List all files in the folder
    #         files = os.listdir(folder_path)

    #         # Iterate through each file and remove it
    #         for file_name in files:
    #             file_path = os.path.join(folder_path, file_name)
    #             if os.path.isfile(file_path):
    #                 os.remove(file_path)
    #             else:
    #                 # If it's a directory, remove it recursively
    #                 os.rmdir(file_path)

    #         print(f"Folder '{folder_path}' cleared successfully.")
    #     except Exception as e:
    #         print(f"An error occurred: {e}")

    # # Example usage:
    # folder_path_to_clear = "MP3_FILE"
    # clear_folder(folder_path_to_clear)

    os.makedirs('partition', exist_ok=True) 
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--protocol-period', type=float, help='Protocol period T in seconds', default=0.25)
    parser.add_argument('-d', '--drop-rate', type=float,
                        help='The message drop rate',
                        default=0)
    args = parser.parse_args()

    for file_name in os.listdir("MP3_FILE"):
        file_path = os.path.join("MP3_FILE", file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)    

    test = Server()
    test.run()
    test.run_failure_detector()
