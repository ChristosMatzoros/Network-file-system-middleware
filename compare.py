# Application
from nfs_client import *
import os
from Color import *

def my_read(fd,buf,n):
	sum = 0
	temp_buf = []
	buf[0] = b''
	while(sum<n):
		res = mynfs_read(fd,temp_buf,n-sum)
		if(res == -2):
			print("No rights for this action")
			return(-2)
		if(res == 0):
			return sum
		buf[0]+=temp_buf[0]
		sum+=res
	return sum

def my_local_read(fd,buf,n):
	sum = 0
	temp_buf = [b'']
	buf[0] = b''
	while(sum<n):
		temp_buf[0] = os.read(fd,n-sum)
		res = len(temp_buf[0])
		if(res == 0):
			return sum
		buf[0]+=temp_buf[0]
		sum+=res
	return sum

start = time.time()

mynfs_setsrv_addr(sys.argv[1],5000)
mynfs_set_cache(8192, 15)
buf = [b'']
nfs_buf = [b'']
local_buf = [b'']

# Open original file and read contents
fd = os.open("uthbig.jpg", os.O_CREAT | os.O_RDWR)
if(fd == -1):
	print("Open failed")
	sys.exit(0)
my_local_read(fd,buf,10000000)
buf_copy = buf.copy()

# Open/Create a NFS file
fd2 = mynfs_open("nfs/nfs_copy.jpg", os.O_CREAT | os.O_RDWR)
if(fd2 == -1):
	print("Open failed")
	sys.exit(0)

# Write to the NFS file
n = len(buf[0])
res = mynfs_write(fd2,buf,n)
if(res == -2):
	print("No rights for this action")

# Open/Create a local file
fd3 = os.open("local_copy.jpg", os.O_CREAT | os.O_RDWR)
if(fd3 == -1):
	print("Open failed")
	sys.exit(0)

# Write to the local file
os.write(fd3, buf_copy[0])

# Read the contents of the two files created
my_read(fd2,nfs_buf,10000000)
my_local_read(fd3,local_buf,10000000)

# Compare the two buffers
if(nfs_buf == local_buf):
	print("Files are the same")
else:
	print("Files differ")
end = time.time()

print("Time:",end-start,"seconds.")
