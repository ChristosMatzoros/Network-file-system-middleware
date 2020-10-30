# NFS Client

import socket
import struct
import pickle
import threading
import time
import sys
import os
from Color import *

POLL_TIME = 10
BLOCK_SIZE = 1024
server_address = ""
server_port = 0
fd = 0
lru_counter = 0							#global LRU counter in order to implement the LRU cache algorithm
freshT = 0
seqno = 0
seqno_mutex = threading.Lock()
list_of_cache_blocks = []				#(fid,content,base,size,LRU,tcheck,tmod)
cache_blocks_mutex = threading.Lock()
list_of_ids = []						#(fd,fid,pos,filename,flags,filesize)
list_of_ids_mutex = threading.Lock()

# Poll the server to inform him about the fds that are currently
# open so that its garbage collector does not delete its entry
def poll_to_server():
	global POLL_TIME

	while(True):
		list_of_ids_mutex.acquire()
		#create a list removing the duplicates of the original list
		temp_list = list([item[1] for item in list_of_ids])
		list_of_ids_mutex.release()
		set_of_ids = []
		for i in temp_list:
			if i not in set_of_ids:
				set_of_ids.append(i)

		list_of_ids_mutex.acquire()
		for id in set_of_ids:
			fid = id.copy()
			flag1,flag2 = readRPC(fid,0,[-1],[0])

		list_of_ids_mutex.release()
		time.sleep(POLL_TIME)

poll_thread = threading.Thread(name = "Poll Server", target=poll_to_server)
poll_thread.daemon = True
poll_thread.start()

def serialize(data):
	result = pickle.dumps(data)
	return result
def deserialize(data):
	result = pickle.loads(data)
	return result

# Update all of the entries of the list for this file with the new file size
def update_file_size(fid,size):
	global list_of_ids
	global list_of_ids_mutex

	list_of_ids_mutex.acquire()
	for item in list_of_ids:
		if item[1] == fid:
			item[5] = size
	list_of_ids_mutex.release()

# Send a new openRPC in the case that the server has rebooted
def file_reopen(fid):
	# Find the file name
	list_of_ids_mutex.acquire()
	for item in list_of_ids:
		if item[1] == fid:
			fname = item[3]
	list_of_ids_mutex.release()

	new_fid = []
	res = openRPC(fname, os.O_RDWR, new_fid)

	if(new_fid[0][0] == -1):
		return -1

	# Update the other entries for the file with the new fid
	list_of_ids_mutex.acquire()
	for item in list_of_ids:
		if item[1] == fid:
			item[1] = new_fid[0].copy()
	list_of_ids_mutex.release()
	return new_fid[0]


# Communication information for the server
def mynfs_setsrv_addr(ipaddr,port):	#(char *ipaddr, int port)
	global server_address
	global server_port

	server_address = ipaddr
	server_port = port

	return 0

# Set the size of the cache, as well as the freshT
def mynfs_set_cache(size, validity):	#(int size, int validity)
	global freshT
	num_of_blocks = size//BLOCK_SIZE
	freshT = validity

	cache_blocks_mutex.acquire()
	for i in range(0,num_of_blocks):
		list_of_cache_blocks.append([[],[],0,0,0,0,0])		#(fid,content,base,size,LRU,tcheck,tmod)
	cache_blocks_mutex.release()

	return(0)

# Open a file from the NFS
def mynfs_open(fname,flags):		#(char *fname, int flags)
	global fd
	fid = []

	# Check if the file with this name is already open
	list_of_ids_mutex.acquire()
	for entry in list_of_ids:
		if(entry[3] == fname):
			#flags: O_CREAT | O_EXCL
			if(flags & (os.O_CREAT | os. O_EXCL) == (os.O_CREAT | os.O_EXCL)):
				return(-1)
			fd+=1
			list_of_ids.append([fd,entry[1].copy(),0,fname,flags,entry[5]])
			print(Color.F_LightRed,list_of_ids,Color.F_Default)
			list_of_ids_mutex.release()

			#flags contains: O_TRUNC
			if(flags & (os.O_TRUNC) == (os.O_TRUNC)):
				res = openRPC(fname, os.O_TRUNC, fid)
				update_file_size(fid.copy(),0)

			return fd
	list_of_ids_mutex.release()

	# If the file with this name is not open perform an openRPC
	res = openRPC(fname, flags, fid)
	if(fid[0][0] == -1):
		return -1


	#create entry for the list list_of_ids
	list_of_ids_mutex.acquire()
	fd+=1
	list_of_ids.append([fd,fid[0].copy(),0,fname,flags,res])
	list_of_ids_mutex.release()

	print(Color.F_LightRed,list_of_ids,Color.F_Default)

	return fd

# Read from a file from the NFS
def mynfs_read(fd,buf,n):			#(int fd, void *buf, size_t n)
	global lru_counter
	global freshT
	# check if this fd exists
	buf.clear()
	found = 0
	fid = []
	pos = 0
	list_of_ids_mutex.acquire()
	for item in list_of_ids:
		if item[0] == fd:
			found = 1
			fid = item[1].copy()
			pos = item[2]
			flags = item[4]
			break
	list_of_ids_mutex.release()
	if(found == 0):
		return -1

	if(not (flags & (os.O_RDONLY) == (os.O_RDONLY) or flags & (os.O_RDWR) == (os.O_RDWR))):
		return(-2);

	#(fid,content,base,size,LRU,tcheck,tmod)
	cache_blocks_mutex.acquire()
	for entry in list_of_cache_blocks:
		if(entry[0] == fid and entry[2] <= pos and entry[2]+BLOCK_SIZE > pos):	#if we find a block with the same fid and the pos is between base and pos+base
			if(time.time() - entry[5] < freshT):	#if we find the block and the time interval is smaller than the freshT then we can fetch this block from the cache
				lru_counter+=1		#refersh the LRU counter for this block
				entry[4] = lru_counter
				if(pos > entry[2]+entry[3]):	#EOF
					cache_blocks_mutex.release()
					return 0
				base = entry[2]
				temp_buf = entry[1][0][(pos-base):(pos-base)+n]
				nofbytes = len(temp_buf)
				item[2]+=nofbytes
				buf.append(temp_buf)
				cache_blocks_mutex.release()
				print(Color.F_LightRed,list_of_ids,Color.F_Default)
				return(nofbytes)
			#if we find the block and the time interval is larger than freshT

			nofbytes = [BLOCK_SIZE]
			temp_buf = [entry[6]]
			tmod,file_size = readRPC(fid,(pos - pos%BLOCK_SIZE),temp_buf,nofbytes)
			while(tmod==-2):
				fid = file_reopen(fid.copy())
				nofbytes = [BLOCK_SIZE]
				temp_buf = [entry[6]]
				tmod,file_size = readRPC(fid,(pos - pos%BLOCK_SIZE),temp_buf,nofbytes)

			update_file_size(fid.copy(),file_size)
			if(nofbytes[0] == 0):	#EOF         ###########
				cache_blocks_mutex.release()
				return 0

			# our copy is valid
			if(temp_buf[0][0] =="OK"):
				entry[5] = time.time()
				lru_counter+=1		#refresh the LRU counter for this block
				entry[4] = lru_counter
				if(pos > entry[2]+entry[3]):	#EOF
					cache_blocks_mutex.release()
					return 0
				base = entry[2]
				temp_buf = entry[1][0][(pos-base):(pos-base)+n]
				nofbytes = len(temp_buf)
				item[2]+=nofbytes
				buf.append(temp_buf)
				cache_blocks_mutex.release()
				print(Color.F_LightRed,list_of_ids,Color.F_Default)
				return(nofbytes)
			else:
				lru_counter+=1
				base = entry[2]
				list_of_cache_blocks.remove(entry)
				list_of_cache_blocks.append([fid.copy(),temp_buf.copy(),(pos - pos%BLOCK_SIZE),nofbytes[0],lru_counter,time.time(),tmod])
				if(pos > entry[2]+entry[3]):	#EOF
					cache_blocks_mutex.release()
					return 0
				buf.append(temp_buf[0][(pos-base):(pos-base)+n])
				nofbytes = len(buf[0])
				item[2]+=nofbytes
				cache_blocks_mutex.release()
				print(Color.F_LightRed,list_of_ids,Color.F_Default)
				return(nofbytes)
			break
	cache_blocks_mutex.release()

	#if we don't find the block in the cache
	nofbytes = [BLOCK_SIZE]
	temp_buf = [0]
	tmod,file_size = readRPC(fid,(pos - pos%BLOCK_SIZE),temp_buf,nofbytes)
	while(tmod==-2):	#server has rebooted, file needs reopening
		nofbytes = [BLOCK_SIZE]
		temp_buf = [0]
		fid = file_reopen(fid.copy())
		tmod,file_size = readRPC(fid,(pos - pos%BLOCK_SIZE),temp_buf,nofbytes)

	update_file_size(fid.copy(),file_size)
	if(nofbytes[0] == 0):	#EOF
		print(Color.F_LightRed,list_of_ids,Color.F_Default)
		return 0


	base = (pos - pos%BLOCK_SIZE)

	cache_blocks_mutex.acquire()
	if(len(list_of_cache_blocks)>0):
		entry = min(list_of_cache_blocks, key=lambda x: x[4])
		list_of_cache_blocks.remove(entry)
		lru_counter+=1

		list_of_cache_blocks.append([fid.copy(),temp_buf.copy(),base,nofbytes[0],lru_counter,time.time(),tmod])

	if(pos > base+nofbytes[0]):	#EOF
		cache_blocks_mutex.release()
		return 0
	buf.append(temp_buf[0][(pos-base):(pos-base)+n])
	nofbytes = len(buf[0])
	item[2]+=nofbytes
	cache_blocks_mutex.release()
	print(Color.F_LightRed,list_of_ids,Color.F_Default)
	return(nofbytes)

# Write to a file from the NFS
def mynfs_write(fd,buf,n):			#(int fd, void *buf, size_t n)
	# check if this fd exists
	global lru_counter
	global freshT
	found = 0
	fid = []
	pos = 0
	list_of_ids_mutex.acquire()
	for item in list_of_ids:
		if item[0] == fd:
			found = 1
			fid = item[1].copy()
			pos = item[2]
			flags = item[4]
			break
	list_of_ids_mutex.release()
	if(found == 0):
		return -1

	if(not (flags & (os.O_WRONLY) == (os.O_WRONLY) or flags & (os.O_RDWR) == (os.O_RDWR))):
		return(-2);

	while True:
		found = 0
		goup = 0
		pos = item[2]
		cache_blocks_mutex.acquire()

		for entry in list_of_cache_blocks:
			#if we find a cache block with the same fid and with between the interval (base,base+BLOCK_SIZE] :
			if(entry[0] == fid and entry[2] <= pos and entry[2]+BLOCK_SIZE > pos):
				if(time.time() - entry[5] < freshT):	#if we find something that is fresh
					base = entry[2]

					#if the size of bytes that we need to write are smaller than the block size
					if(n <= BLOCK_SIZE-(pos-base)):
						#refresh the cache block with new information
						lru_counter+=1
						entry[4] = lru_counter
						temp = b''
						temp+=entry[1][0][:(pos-base)]
						temp+=buf[0]
						temp+=entry[1][0][(pos-base)+n:]
						entry[1][0] = temp
						entry[3] = len(temp)

						tmod,file_size = writeRPC(fid,pos,buf,n)
						while(tmod==-2):
							fid = file_reopen(fid.copy())
							tmod,file_size = writeRPC(fid,pos,buf,n)
						update_file_size(fid.copy(),file_size)

						entry[5] = time.time()	#tcheck
						entry[6] = tmod			#tmod
						list_of_ids_mutex.acquire()
						item[2]+=n					#assign the new pos for the spacific item in the list_of_ids
						list_of_ids_mutex.release()
						cache_blocks_mutex.release()
						print(Color.F_LightRed,list_of_ids,Color.F_Default)
						return 1
					else:	#the size of bytes that we need to write are equal to block size
						#refresh the cache block with new information
						lru_counter+=1
						entry[4] = lru_counter
						temp = b''
						temp+=entry[1][0][:(pos-base)]
						temp+=buf[0][:BLOCK_SIZE-(pos-base)]
						temp+=entry[1][0][(pos-base)+BLOCK_SIZE-(pos-base):]
						entry[1][0] = temp
						entry[3] = len(temp)
						tmod,file_size = writeRPC(fid,pos,buf.copy(),BLOCK_SIZE-(pos-base))

						while(tmod==-2):
							fid = file_reopen(fid.copy())
							tmod,file_size = writeRPC(fid,pos,buf.copy(),BLOCK_SIZE-(pos-base))
						update_file_size(fid.copy(),file_size)

						entry[5] = time.time()
						entry[6] = tmod
						buf[0] = buf[0][BLOCK_SIZE-(pos-base):]
						n = len(buf[0])	#assign n with the new length of the buffer

						list_of_ids_mutex.acquire()
						item[2]+=BLOCK_SIZE-(pos-base)	#assign the new pos for the spacific item in the list_of_ids
						list_of_ids_mutex.release()

						cache_blocks_mutex.release()
						goup = 1
						break
				else:
					found = 1
					break
		if(len(list_of_cache_blocks)>0):
			if(goup == 1):
				continue
			if(found == 0):
				entry = min(list_of_cache_blocks, key=lambda x: x[4])	#find the entry with the smallest LRU counter value(last used cache block)

		base = (pos // BLOCK_SIZE)*BLOCK_SIZE

		#if the size of bytes that we need to write are smaller than the block size
		if(n <= BLOCK_SIZE-(pos-base)):
			write_buf = buf
			tmod,file_size = writeRPC(fid,pos,write_buf,n)
			while(tmod==-2):
				fid = file_reopen(fid.copy())
				tmod,file_size = writeRPC(fid,pos,write_buf,n)
			update_file_size(fid.copy(),file_size)

			#refresh the cache block with new information
			if(len(list_of_cache_blocks)>0):
				entry[0] = fid.copy()
				entry[1] = write_buf
				entry[2] = base
				entry[3] = len(write_buf[0])
				lru_counter+=1
				entry[4] = lru_counter
				entry[5] = time.time()		#tcheck
				entry[6] = tmod				#tmod

			list_of_ids_mutex.acquire()
			item[2]+=n						#assign the new pos for the spacific item in the list_of_ids
			list_of_ids_mutex.release()

			cache_blocks_mutex.release()
			print(Color.F_LightRed,list_of_ids,Color.F_Default)
			return 1
		else:	#the size of bytes that we need to write are equal to block size
			#write a whole block of information with writeRPC
			write_buf = [buf[0][:BLOCK_SIZE-(pos-base)]]
			tmod,file_size = writeRPC(fid,pos,write_buf,BLOCK_SIZE-(pos-base))
			while(tmod==-2):
				fid = file_reopen(fid.copy())
				tmod,file_size = writeRPC(fid,pos,write_buf,BLOCK_SIZE-(pos-base))
			update_file_size(fid.copy(),file_size)

			#append the new information
			if(len(list_of_cache_blocks)>0):
				entry[0] = fid.copy()
				entry[1] = write_buf
				entry[2] = base
				entry[3] = len(write_buf[0])
				lru_counter+=1
				entry[4] = lru_counter
				entry[5] = time.time()	#tcheck
				entry[6] = tmod			#tmod

			buf[0] = buf[0][BLOCK_SIZE-(pos-base):]
			n = len(buf[0])	#assign n with the new length of the buffer
			cache_blocks_mutex.release()

			list_of_ids_mutex.acquire()
			item[2]+=BLOCK_SIZE-(pos-base)	#assign the new pos for the spacific item in the list_of_ids
			list_of_ids_mutex.release()
			continue
	return(1)

def mynfs_seek(fd,pos,whence):
	found = 0
	list_of_ids_mutex.acquire()
	#search the entry of the list_of_ids that contains the specific application fd
	for item in list_of_ids:
		if item[0] == fd:
			found = 1
			break
	list_of_ids_mutex.release()

	#if it does not exist return failure code
	if(found == 0):
		return -1

	if(whence == "SEEK_SET"):	#the new position of fd is equal to pos
		item[2] = pos
	elif(whence == "SEEK_CUR"):	#the new position of fd is equal to the current position of fd + the pos value
		item[2] += pos
	elif(whence == "SEEK_END"):	#the new position of fd is equal to the position of the last byte of the file + the pos value
		item[2] = item[5] + pos
	else:
		print("Wrong flags in mynfs_seek()")

def mynfs_close(fd):
	found = 0
	list_of_ids_mutex.acquire()
	#search the entry of the list_of_ids that contains the specific application fd
	for item in list_of_ids:
		if item[0] == fd:
			found = 1
			break
	list_of_ids_mutex.release()

	#if it does not exist return failure code
	if(found == 0):
		return -1

	#if it exists remove it from the list_of_ids
	list_of_ids.remove(item)
	return(0)


def openRPC(fname,flags,fid):		#(char *fname, int flags, int *fid)
	global seqno
	global seqno_mutex

	#create a new seqno for the message
	seqno_mutex.acquire()
	seqno += 1
	my_seqno = seqno
	seqno_mutex.release()

	msg = serialize(["OPEN_RPC",my_seqno,[fname,flags]])

	# Create UDP socket
	open_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
	open_socket.settimeout(1)

	while True:
		try:
			open_socket.sendto(msg,(server_address,server_port))	#send the message to the file server
			response, manager_address = open_socket.recvfrom(2048)	#recieve a response from the server
			response = deserialize(response)
			while(response[1]!=my_seqno):							#if the recieved message has different seqno repeat the receive
				response, manager_address = open_socket.recvfrom(2048)
				response = deserialize(response)
		except socket.timeout as e:
			continue
		break

	if(response[0] == "OPEN_RPC_ACK"):
		#append information to the in-out parameters
		fid.append(response[2].copy())
		return(response[3])	#return size_of_file
	else:
		return(-1)

def readRPC(fid,pos,buf,nofbytes):
	global seqno
	global seqno_mutex

	#create a new seqno for the message
	number_of_bytes = nofbytes.pop(0)
	seqno_mutex.acquire()
	seqno += 1
	my_seqno = seqno
	seqno_mutex.release()


	tmod = buf[0]
	msg = serialize(["READ_RPC",my_seqno,[fid,pos,0,number_of_bytes,tmod]])
	buf.remove(tmod)

	# Create UDP socket
	read_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
	read_socket.settimeout(1)

	while True:
		try:
			read_socket.sendto(msg,(server_address,server_port))	#send the message to the file server
			response, manager_address = read_socket.recvfrom(2048)	#recieve a response from the server
			response = deserialize(response)
			while(response[1]!=my_seqno):							#if the recieved message has different seqno repeat the receive
				response, manager_address = read_socket.recvfrom(2048)
				response = deserialize(response)
		except socket.timeout as e:
			continue
		break

	if(response[0] == "READ_RPC_ACK"):
		if(response[2]==-1):	#if the message equals to "-1" return code [-2,-2] in order to reopen the file later
			return([-2,-2])
#append information to the in-out parameters
		message = response[2][0]
		len = response[2][1]
		tmod = response[3]
		buf.append(message)
		nofbytes.append(len)
		#return [tmod,size_of_file]
		return([tmod,response[4]])
	else:
		return([-1,-1])

def writeRPC(fid,pos,buf,nofbytes):	#(int fid, int pos, const void *buf, int nofbytes)
	global seqno
	global seqno_mutex

	seqno_mutex.acquire()
	seqno += 1
	my_seqno = seqno
	seqno_mutex.release()
	msg = serialize(["WRITE_RPC",my_seqno,[fid,pos,buf[0],nofbytes]])

	write_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
	write_socket.settimeout(1)

	while True:
		try:
			write_socket.sendto(msg,(server_address,server_port))	#send the message to the file server
			response, manager_address = write_socket.recvfrom(2048)	#recieve a response from the server
			response = deserialize(response)
			while(response[1]!=my_seqno):							#if the recieved message has different seqno repeat the receive
				response, manager_address = write_socket.recvfrom(2048)
				response = deserialize(response)
		except socket.timeout as e:
			continue
		break

	if(response[0] == "WRITE_RPC_ACK"):
		if(response[2]==-1):	#if the message equals to "-1" return code [-2,-2] in order to reopen the file later
			return([-2,-2])
		#append information to the in-out parameters
		buf[0] = response[4]
		tmod = response[3]
		#return [tmod,size_of_file]
		return([tmod,response[5]])
	else:
		return([-1,-1])
