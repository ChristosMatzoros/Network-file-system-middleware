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

mynfs_setsrv_addr(sys.argv[1],5000)
cache_size = 0
if(sys.argv[2] == "1"):
	cache_size = 16344

mynfs_set_cache(cache_size, 15)
buf = [b'']
total_start = time.time()

fd = mynfs_open("uth.png", os.O_CREAT | os.O_RDWR)
if(fd == -1):
	print("Open failed")
	sys.exit(0)

start = time.time()
my_read(fd,buf,10000000)
end = time.time()
t1 = end-start

fd2 = mynfs_open("copy1.png", os.O_CREAT | os.O_RDWR)
if(fd2 == -1):
	print("Open failed")
	sys.exit(0)

mynfs_seek(fd,0,"SEEK_SET")
start = time.time()
my_read(fd,buf,10000000)
end = time.time()
t2 = end-start

n = len(buf[0])
res = mynfs_write(fd2,buf,n)
if(res == -2):
	print("No rights for this action")

total_end = time.time()

print("Time first read:",t1,"seconds.")
print("Time second read:",t2,"seconds.")
print("Time:",total_end-total_start,"seconds.")
