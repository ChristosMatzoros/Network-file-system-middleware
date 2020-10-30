# NFS Server
import socket
import struct
import pickle
import threading
import time
import sys
import os
from Color import *
BLOCK_SIZE = 1024

server_boot_time = time.time()
fid = [0,server_boot_time]
garbage_collect_interval = 20			#used in order to clear the entries of servers_list after a time
requests_list = []						#requests_list is used to achieve demultiplexing of the recieved packets
server_list = []						#server list contains information of this form [fid,fd]
requests_list_mutex = threading.Lock()
server_list_mutex = threading.Lock()

# Create UDP socket
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
udp_socket.bind(('', 5000))

#Functions serialize and deserialize are using pickle library to serialize the packet
#to binary and deserialize from binary to a specific data structure
def serialize(data):
	result = pickle.dumps(data)
	return result
def deserialize(data):
	result = pickle.loads(data)
	return result

#udp_listener listens to UDP multicast for messages
def udp_listener():
	while True:
		msg,(client_ip, client_port) = udp_socket.recvfrom(2048)
		msg = deserialize(msg)
		msg.append([client_ip,client_port])
		requests_list_mutex.acquire()
		requests_list.append(msg)
		requests_list_mutex.release()

# Garbage Collector: if noone has read from or written to a file
# with a specific file id, delete its entry from the list
def garbage_collector():
	global garbage_collect_interval

	while(True):
		server_list_mutex.acquire()
		for item in server_list:
			if(time.time() - item[2] > garbage_collect_interval):
				server_list.remove(item)
		server_list_mutex.release()
		time.sleep(2)

udp_thread = threading.Thread(name = "UDP Listener", target=udp_listener)
garbage_collector_thread = threading.Thread(name = "Garbage_Collector", target=garbage_collector)
udp_thread.daemon = True
garbage_collector_thread.daemon = True
udp_thread.start()
garbage_collector_thread.start()

while True:
	requests_list_mutex.acquire()
	while(not requests_list):
		requests_list_mutex.release()
		requests_list_mutex.acquire()
	requests_list_mutex.release()

	#Pop a message from the requests_list
	requests_list_mutex.acquire()
	request = requests_list.pop(0)
	requests_list_mutex.release()



	if(request[0] == "OPEN_RPC"):
		print(request[0], request[2])
		#retrieve information
		seqno = request[1]
		filename = request[2][0]
		flags = request[2][1]
		client_addr = request[3][0]
		client_port = request[3][1]

		#if the flags is equal to O_TRUNC we truncate the file and we send the new
		#information for this file back to the client
		if(flags == os.O_TRUNC):
			os.ftruncate(fd,0)
			info = os.fstat(fd)
			size_of_file = info.st_size
			udp_socket.sendto(serialize(["OPEN_RPC_ACK",seqno,fid,size_of_file]),(client_addr,client_port))	#(TAG,seqno,fid,size_of_file)
			continue
		try:
			fd = os.open(filename, flags)
		except FileExistsError:
			udp_socket.sendto(serialize(["OPEN_RPC_ACK",seqno,-1]),(client_addr,client_port))
			continue

		fid[0]+=1	#create an fid for the new file

		server_list_mutex.acquire()
		server_list.append([fid.copy(),fd,time.time()])		#append the relation between the fid and the fd
		server_list_mutex.release()

		info = os.fstat(fd)
		size_of_file = info.st_size

		#send the information for the new file back to the client
		udp_socket.sendto(serialize(["OPEN_RPC_ACK",seqno,fid,size_of_file]),(client_addr,client_port))	#(TAG,seqno,fid,size_of_file)

	elif(request[0] == "WRITE_RPC"):
		print(request[0], request[2][0])
		seqno = request[1]
		client_addr = request[3][0]
		client_port = request[3][1]
		server_list_mutex.acquire()
		found = 0
		#search for the entry with the specific fid
		for item in server_list:
			if item[0] == request[2][0]:
				found = 1
				fd = item[1]
				item[2] = time.time()
				break
		server_list_mutex.release()
		if(found == 0):
			#send acknowledgement back to the server with the value "-1" in order to inform the
			#client that there is no enrty with the specific id
			udp_socket.sendto(serialize(["WRITE_RPC_ACK",seqno,-1]),(client_addr,client_port))
			continue

		os.lseek(fd, request[2][1], os.SEEK_SET)
		number_of_bytes_written = os.write(fd, request[2][2])	#write data to the file
		os.fsync(fd)
		info = os.fstat(fd)
		size_of_file = info.st_size

		#retrieve the changed block and send it back to the client
		os.lseek(fd, request[2][1] - (request[2][1]%BLOCK_SIZE), os.SEEK_SET)
		block_buf = os.read(fd, BLOCK_SIZE)
		udp_socket.sendto(serialize(["WRITE_RPC_ACK",seqno,fid,info.st_mtime,block_buf,size_of_file]),(client_addr,client_port))	#(TAG,seqno,fid,info.st_mtime,block_buf,size_of_file)

	elif(request[0] == "READ_RPC"):
		print(request[0], request[2][0])
		seqno = request[1]
		client_addr = request[3][0]
		client_port = request[3][1]
		server_list_mutex.acquire()
		found = 0
		#search for the entry with the specific fid
		for item in server_list:
			if item[0] == request[2][0]:
				found = 1
				fd = item[1]
				item[2] = time.time()
				break
		server_list_mutex.release()

		if(found == 0):
			#send acknowledgement back to the server with the value "-1" in order to inform the
			#client that there is no enrty with the specific id
			udp_socket.sendto(serialize(["READ_RPC_ACK",seqno,-1]),(client_addr,client_port))
			continue

		pos = request[2][1]
		nofbytes = request[2][3]
		client_tmod = request[2][4]
		info = os.fstat(fd)
		size_of_file = info.st_size
		if(info.st_mtime == client_tmod):
			#if the tmod time has not changed, send "OK" message back to the client,
			#to inform that the requested block haven't changed
			udp_socket.sendto(serialize(["READ_RPC_ACK",seqno,[["OK"],len("OK")],info.st_mtime,size_of_file]),(client_addr,client_port))	#(TAG,seqno,[["OK"],len("OK")],info.st_mtime,size_of_file)
			continue

		#if the file has changed, read the changed block from the file
		os.lseek(fd, pos, os.SEEK_SET)
		buf = os.read(fd, nofbytes)
		nofbytes = len(buf)
		os.fsync(fd)
		info = os.fstat(fd)
		size_of_file = info.st_size

		#send a message back to the client that contains the changed block
		udp_socket.sendto(serialize(["READ_RPC_ACK",seqno,[buf,nofbytes],info.st_mtime,size_of_file]),(client_addr,client_port))	#(TAG,seqno,[buf,nofbytes],info.st_mtime,size_of_file)
	else:
		print("Wrong command!")
