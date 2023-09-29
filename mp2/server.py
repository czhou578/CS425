import threading
import sys
import datetime
import random
import socket
import json
import config
import time
import traceback
import select
import os
import multiprocessing

stop_flag = False
event = threading.Event()
joined_success_flag = False #used to signal whether a process successfully received joined messages

class MembershipList:
    def __init__(self, node_id, node_status, host_name, hb):
        self.memberList = {
            host_name: {
                'node_id': node_id,
                'status': 'LEFT',
                'hb': hb,
                'time_stamp': datetime.datetime.now().strftime("%H:%M:%S"),
                'incarNum': 0
            }
        }

class Server:
    def __init__(self, host, port, log_file_path):
        self.node_id = random.randint(1, 10000)
        self.host = host
        self.port = port
        self.hb = 0
        self.introducer = config.INTRODUCER
        self.log_file_path = log_file_path 
        self.membership_list = MembershipList(node_id=self.node_id, node_status='LEFT', host_name=host, hb=self.hb)
        self.neighbors = config.NEIGHBORS[host]
        self.timeoutTable = {}
        self.timeoutLock = threading.Lock() #used in timeout function
        self.hblock = threading.Lock()        
        self.nodeId_failed_set = set() #keep track of node failures
        self.suspicion_mode = False

    ''' 
        function for increasing heartbeat of this machine.
    '''

    def increase_hb(self):
        while True:
            with self.hblock:
                self.membership_list.memberList[self.host]['hb'] += 1
            time.sleep(0.4)   

    '''  
        function for joining the distributed system. Once it receives the
        'ACCEPT' message from the introducer, then it will join the system.
    '''  

    def join(self):
        global joined_success_flag
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.bind((self.host, self.port))
                msg_to_send = 'JOIN'
                while joined_success_flag == False:
                    s.sendto(json.dumps(msg_to_send).encode('utf-8'), (self.introducer, self.port))
                    payload, client_address = s.recvfrom(4096)
                    payload = json.loads(payload)
                    if payload == 'ACCEPT':
                        joined_success_flag = True
                        break

                start_event.set()
                    
        except Exception as e:
            print("error, ", str(e))
        s.close()

    '''  
        function for leaving the system. 
    '''
    
    def leave(self):
        print('leave the system')
        entry = self.membership_list.memberList[self.host]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                entry['status'] = 'LEFT'
                entry['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                msg_to_send = {
                    self.host: {
                        'node_id': entry['node_id'],
                        'status': entry['status'],
                        'time_stamp': entry['time_stamp'],
                        'msg_type': 'PING'
                    }
                }
                
                for host in self.neighbors:
                    s.sendto(json.dumps(msg_to_send).encode('utf-8'), (host, port))

                time.sleep(0.3)
                stop_event.set()
                        
        except Exception as e:
            print(str(e))
        
        if os.path.exists(self.log_file_path):
            # If it exists, delete the log file
            os.remove(self.log_file_path)

    '''  
    this is the function for receiving gossip messages from other machines. 
    It will call the update_member or update_member_suspicious functions after receiving
    the initial gossips, which will then perform the membership list updates.
    '''

    def recv_message(self):
        entry = self.membership_list.memberList[self.host]
        
        start_event.wait() 
        while not stop_event.is_set():
            if stop_flag == False:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.bind((self.host, self.port))
        
                entry['status'] = 'RUNNING'
                payload, client_address = s.recvfrom(4096)
                recv_host_name, _, _ = socket.gethostbyaddr(client_address[0])
                payload = json.loads(payload)

                if (self.introducer == self.host and payload == 'JOIN'): #only for introducer
                        msg_to_send = 'ACCEPT'
                        s.sendto(json.dumps(msg_to_send).encode('utf-8'), (recv_host_name, port))
                        print("send out")
                else:
                    if self.suspicion_mode == True:
                        self.update_member_suspicious(payload, recv_host_name)
                        continue

                    self.update_member(payload, recv_host_name)

        s.close()

    ''' 
        Sends the membership list of the current machine to the other machines in the network.
    '''
    
    def send_message(self):
        entry = self.membership_list.memberList
        start_event.wait() 
        while not stop_event.is_set():
            if stop_flag == False:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        entry[self.host]['status'] = 'RUNNING'
                        entry[self.host]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                        msg_to_send = {
                            'payload': entry,
                        }

                        for host in self.neighbors:
                            s.sendto(json.dumps(msg_to_send).encode('utf-8'), (host, port))

                        if self.suspicion_mode == True:
                            time.sleep(0.2)
                            continue
                    
                    time.sleep(0.5)

                except Exception as e:
                    print("error, ", str(e))
                s.close()

    ''' 
        updates the time table, which keeps track of the time of the most recently received
        update to a host. If the difference between the current time and the stored time in the
        timeout table is greater then the parameter amount, membership list status of the node
        will be updated accordingly.
    '''
    def time_checker(self):
        global previous_suspected
        while True:
            try:
                self.timeoutLock.acquire()
                for host in self.timeoutTable.keys():
                    if host in self.membership_list.memberList and host != self.host:

                        host_timeout_time = self.timeoutTable[host]
                        now = datetime.datetime.now()

                        time_delta = now - host_timeout_time

                        with open(log_file_path, 'a') as log_file:

                            if self.suspicion_mode == True and time_delta.seconds >= 10. and self.membership_list.memberList[host]['status'] == 'SUSPECTED':
                                log_file.write(f'Existing membership list: {self.membership_list.memberList} \n')
                                self.nodeId_failed_set.add(self.membership_list.memberList[host]['node_id'])

                                status = 'FAILED'
                                log_file.write(f'This host was removed from Membership List: {host} with status {status} \n')
                                del self.membership_list.memberList[host]

                                log_file.write(f'New membership list: {self.membership_list.memberList} \n\n')
                                continue


                            elif self.suspicion_mode == True and time_delta.seconds >= 2.:
                                self.membership_list.memberList[host]['status'] = 'SUSPECTED'
                                continue

                            # ------------------------------------------------------------------->

                            if time_delta.seconds >= 9. and self.membership_list.memberList[host]['status'] == 'FAILED':
                                log_file.write(f'Existing membership list: {self.membership_list.memberList} \n')
                                log_file.write(f'existing time_stamp for {host}: {self.timeoutTable[host]} \n')
                                log_file.write(f'current time is {now} and difference is: {time_delta} \n')

                                self.nodeId_failed_set.add(self.membership_list.memberList[host]['node_id'])
                                
                                status = 'FAILED'
                                
                                log_file.write(f'This host was removed from Membership List: {host} with status {status} \n')
                                
                                del self.membership_list.memberList[host]
                                log_file.write(f'New membership list: {self.membership_list.memberList} \n\n')

                            elif time_delta.seconds > 2.:
                                self.membership_list.memberList[host]['status'] = 'FAILED'
                                

                self.timeoutLock.release()

            except Exception:
                traceback.print_exc()
    
    ''' 
        updates the membership list of the current machine when the suspicion protocol is running.
        It has to compare the incarnation numbers to distinguish between which messages to pay 
        attention to. 
    '''
    
    def update_member_suspicious(self, payload, recv_host_name):
        memList = self.membership_list.memberList
        if 'payload' in payload and payload['payload'][recv_host_name]['node_id'] not in self.nodeId_failed_set:
            self.timeoutLock.acquire()

            with open(log_file_path, 'a') as log_file:
                test = payload['payload']
                for host in test:
                    if test[host]['status'] == 'SUSPECTED':
                        log_file.write(f'from {recv_host_name}, suspected host: {host}, {test[host]} \n\n')            

            for member in payload['payload']:

                if member in memList.keys():
                    if memList[member]['status'] == 'FAILED' and payload['payload'][member]['hb'] <= memList[member]['hb']:
                        continue

                    if memList[member]['node_id'] == payload['payload'][member]['node_id']:

                        if payload['payload'][member]['status'] == 'LEFT':
                            self.nodeId_failed_set.add(payload['payload'][member]['node_id'])
                            memList[member]['status'] = payload['payload'][member]['status']

                        else:
                            #---------------------------------> NOT LEFT
                            #received failed message
                            if payload['payload'][member]['status'] == 'FAILED':
                                continue

                            #incarNum is greater
                            if payload['payload'][member]['incarNum'] > memList[member]['incarNum']:
                                memList[member]['status'] == 'RUNNING'
                                memList[member]['incarNum'] = payload['payload'][member]['incarNum']

                                if 'hb' in memList[member] and memList[member]['hb'] < payload['payload'][member]['hb']: #if heartbeat is higher
                                    memList[member]['status'] = payload['payload'][member]['status']
                                    memList[member]['hb'] = payload['payload'][member]['hb']
                                    memList[member]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                                    self.timeoutTable[member] = datetime.datetime.now()
                                
                            #incarNum is same    
                            elif payload['payload'][member]['incarNum'] == memList[member]['incarNum']:

                                if memList[member]['status'] == 'RUNNING' and payload['payload'][member]['status'] == 'SUSPECTED':
                                    memList[member]['status'] == 'SUSPECTED'

                                    if 'hb' in memList[member] and memList[member]['hb'] < payload['payload'][member]['hb']: #if heartbeat is higher
                                        memList[member]['status'] = payload['payload'][member]['status']
                                        memList[member]['hb'] = payload['payload'][member]['hb']
                                        memList[member]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                                        self.timeoutTable[member] = datetime.datetime.now()

                                if memList[member]['status'] == 'SUSPECTED' and payload['payload'][member]['status'] == 'RUNNING':

                                    if 'hb' in memList[member] and memList[member]['hb'] < payload['payload'][member]['hb']: #if heartbeat is higher
                                        memList[member]['status'] = payload['payload'][member]['status']
                                        memList[member]['hb'] = payload['payload'][member]['hb']
                                        memList[member]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                                        self.timeoutTable[member] = datetime.datetime.now()


                                if memList[member]['status'] == 'RUNNING' and payload['payload'][member]['status'] == 'RUNNING':
                                    if 'hb' in memList[member] and memList[member]['hb'] < payload['payload'][member]['hb']: #if heartbeat is higher
                                        memList[member]['status'] = payload['payload'][member]['status']
                                        memList[member]['hb'] = payload['payload'][member]['hb']
                                        memList[member]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                                        self.timeoutTable[member] = datetime.datetime.now()


                            if member == self.host and payload['payload'][member]['status'] == 'SUSPECTED': #self.host is suspected case
                                memList[member]['incarNum'] += 1

                                if 'hb' in memList[member] and memList[member]['hb'] < payload['payload'][member]['hb']: #if heartbeat is higher
                                    memList[member]['status'] = payload['payload'][member]['status']
                                    memList[member]['hb'] = payload['payload'][member]['hb']
                                    memList[member]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                                    self.timeoutTable[member] = datetime.datetime.now()
                else:
                    if payload['payload'][member]['node_id'] in self.nodeId_failed_set:
                        continue

                    self.membership_list.memberList[member] = payload['payload'][member]
                    self.timeoutTable[member] = datetime.datetime.now()
                    
            self.timeoutLock.release()

    ''' 
        updates the membership list of the current machine when the regular gossip is running. 
        It merges row by row between the incoming and current membership lists, and adds new hosts
        if needed. Also sets the time in the time checker table with the current time. Checks the
        heartbeat of the incoming hosts in the received payload with the heartbeats stored inside the
        membership list.
    '''

    def update_member(self, payload, recv_host_name):
        memList = self.membership_list.memberList
        if 'payload' in payload and payload['payload'][recv_host_name]['node_id'] not in self.nodeId_failed_set:
            self.timeoutLock.acquire()

            with open(log_file_path, 'a') as log_file:
                test = payload['payload']
                log_file.write(f'received {test} \n\n')                         
                for host in test:
                    if test[host]['status'] == 'FAILED':
                        log_file.write(f'from {recv_host_name}, failed host: {host}, {test[host]} \n') 
                        log_file.write(f'current membership entry for {host}: {test[host]} \n\n')                         

            for member in payload['payload']:
                if member in memList.keys():
                    if memList[member]['status'] == 'FAILED' and payload['payload'][member]['hb'] <= memList[member]['hb']:
                        continue

                    if memList[member]['node_id'] == payload['payload'][member]['node_id']:
                        if payload['payload'][member]['status'] == 'LEFT':
                            self.nodeId_failed_set.add(payload['payload'][member]['node_id'])
                            self.timeoutTable[member] = 6
                            memList[member]['status'] = payload['payload'][member]['status']
                        else:

                            if 'hb' in memList[member] and memList[member]['hb'] < payload['payload'][member]['hb']:
                                memList[member]['status'] = payload['payload'][member]['status']
                                memList[member]['hb'] = payload['payload'][member]['hb']
                                memList[member]['time_stamp'] = datetime.datetime.now().strftime("%H:%M:%S")
                                self.timeoutTable[member] = datetime.datetime.now()
                else:
                    if payload['payload'][member]['node_id'] in self.nodeId_failed_set:
                        continue

                    self.membership_list.memberList[member] = payload['payload'][member]
                    self.timeoutTable[member] = datetime.datetime.now()
                    
            self.timeoutLock.release()

    def user_input(self):
        while True: 
            arg = input('Enter node command: ')
            if arg == "-join":
                self.join()
                print('joined')
            elif arg == "-leave":
                self.leave()
                print('left')
            elif arg == "-list_self":
                print("Node id: ", self.node_id)
            elif arg == "-list_mem":
                print("Membership List: ", self.membership_list.memberList.keys())
            elif arg == "-susp":
                self.suspicion_mode = True
                print('enable suspicion')
            elif arg == "-dis_susp":
                print('disable suspicion')
            else:
                print('Invalid argument, try again')
    
    def execute(self):
        user_input_thread = threading.Thread(target=self.user_input)
        send_thread = threading.Thread(target=self.send_message)
        recv_thread = threading.Thread(target=self.recv_message)
        time_thread = threading.Thread(target=self.time_checker)
        hb_thread = threading.Thread(target=self.increase_hb)


        user_input_thread.start()
        hb_thread.start()
        send_thread.start()
        recv_thread.start()
        time_thread.start()

        send_thread.join()
        recv_thread.join()
        time_thread.join()
        user_input_thread.join()

log_file_path = "vm.log"

if not os.path.exists(log_file_path):
    # If it doesn't exist, create the log file
    with open(log_file_path, 'a') as log_file:
        pass
else: 
    os.remove(log_file_path)

host = socket.gethostname()
port = 55558

## take responsiblitiy for: 1. Node can only send/recieve after join. 2. Node will stop send/recieve after leave
start_event = threading.Event()
stop_event = threading.Event()

if host == config.INTRODUCER:
    start_event.set()

server = Server(host, port, log_file_path)
server.execute()

