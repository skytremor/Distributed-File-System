###############################################################################
#
# Filename: copy.py
# Author: Christian Rodriguez and Jose R. Ortiz
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path
import random

from Crypto.Cipher import ARC4
from Packet import *

global key 
key = '852138335678754123456789752123456789'

def chunkiefier(fsize, amount_of_nodes):
  avg = len(fsize) / float(amount_of_nodes)
  out = []
  last = 0.0

  while last < len(fsize):
    out.append(fsize[int(last):int(last + avg)])
    last += avg

  return out

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""
	#----------------------
	# Create a connection to the data server
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try: 
		sock.connect(address)
	except:
		print 'Could not connect to meta-data server! Are you sure you have the right address?'

	# Read file
	f = open(path, 'rb')
	file_data = f.read()
	f.close()
	
	# Create a Put packet with the fname and the length of the data, and sends it to the metadata server 
	#-----------------------
	p = Packet()
	p.BuildPutPacket(fname, len(file_data))
	sock.sendall(p.getEncodedPacket())

	nodes = sock.recv(1024)

	sock.close()
	#-----------------------

	#-----Data node Information--------
	if nodes == "DUP":
		print "Duplicate error. This file already exists!"
		return

	# Get the list of data nodes.
	p.DecodePacket(nodes)
	data_nodes = p.getDataNodes()

	if len(data_nodes) == 0:
		print 'No data nodes available!'
		return
	#----------------------------------
	#Information pertaining nodes and file.
	dnode_amount = len(data_nodes) #amount of nodes online atm
	#----------------------------------

	#----Splitting in chunks-----------
	# Divide the file in blocks
	variable = chunkiefier(file_data, dnode_amount) #divides the file into the amount of nodes
	#----------------------------------

	#-------Combination of division and sending-----------
	for i in range(dnode_amount):
		#----------------------------------------
		#Dividing each node block into 1024 blocks & attempting to connect to node
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			sock.connect((data_nodes[i][0], data_nodes[i][1]))
		except: 
			print "Could not connect to node server."
		#----------------------------------------

		#----------------------------------------
		#Packet with information about our file	
		p2 = Packet()
		p2.BuildPutPacket(fname, len(file_data))
	 	sock.sendall(p2.getEncodedPacket())
	 	ack = sock.recv(1024)
	 	#----------------------------------------

	 	#----------------------------------------
	 	#Sending Node the size of the data they will receive
	 	node_size = len(variable[i]) #Amount of data data node is waiting for
	 	sock.send(str(node_size))
	 	ack = sock.recv(1024) #Ensures one send per recv
	 	#----------------------------------------

	 	#----------------------------------------
		sock.send('SENDINGBLOCKSNOW')
		ready = sock.recv(1024)
	 	#----------------------------------------
	 	if ready == 'OKSENDTHEDATA':
	 		obj1 = ARC4.new(str(key)) #Generating encryption object with key
		 	#Sending packets to each node server
			sock.sendall(obj1.encrypt(str(variable[i]))) #Sending the encrypted data through socket.
			acks = sock.recv(1024) #Acknowledge to ensure one send per recv
		else:
			print 'Data node is not ready to receive the data.'
			exit(0)
			print ready
		#----------------------------------------
		
		#----------------------------------------
		#Receiving block id after all data has been sent to data node
		print "Done distributing data to node", i+1, "!"
		sock.send("OK")
		block_id = sock.recv(1024) #Receiving
		data_nodes[i].append(block_id) #Adding the block id to the data node information array
		sock.close() #Closing the socket since we do not need it anymore for that particular data node
		#----------------------------------------
	#--------------------------------------------

	#--------------------------------------------
	# Talking with data server
	# Notify the metadata server where the blocks are saved.
	metasock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	metasock.connect(address)

	# Create a packet object to register data blocks into meta data
	p = Packet()

	p.BuildDataBlockPacket(fname, data_nodes)

	metasock.sendall(p.getEncodedPacket())
	metasock.close()
	#--------------------------------------------
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""
	file_size = 0
	#--------------------------------------------
   	# Contact the metadata server to ask for information of fname
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(address)
	#--------------------------------------------

	#--------------------------------------------
	# Getting block ids from meta server
	p = Packet()
	p.BuildGetPacket(fname)
	sock.sendall(p.getEncodedPacket())
	#--------------------------------------------

	#--------------------------------------------
	# Receiving the meta server response
	bids = sock.recv(1024)
	sock.close() #We no longer need anything from the meta server
	p.DecodePacket(bids)
	data_nodes = p.getDataNodes()
	#--------------------------------------------

	#--------------------------------------------
	# File to store in incoming data
	f = open(path, 'wb')
	#--------------------------------------------

	#--------------------------------------------
	# Going through each node and receiving their blocks
	for i in data_nodes:

		#--------------------------------------------
		# Connecting to node
		sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			sock2.connect((i[0], i[1]))
		except: 
			print "Could not connect to datanode", i
		#--------------------------------------------

		#--------------------------------------------	
		# Letting datanode know we want some data.
		p.BuildGetDataBlockPacket(i[2])
		sock2.sendall(p.getEncodedPacket())
		#--------------------------------------------

		#--------------------------------------------
		# First thing received is size of data node will send us
		node_size = sock2.recv(1024)
		node_size = int(node_size)
		sock2.send("OK") #One recv per send
		#--------------------------------------------

		#--------------------------------------------
		# Get data in 1024 size parts
		data = ''
		while(len(data) < node_size):
			block = sock2.recv(node_size)
			data = data + block
		sock2.send("OK")	
		#--------------------------------------------

		#Decrypt file
		obj2 = ARC4.new(str(key))
		data = obj2.decrypt(data)

		# Save the file
		f.write(data)
		sock2.close()
		file_size = file_size + len(data)
	#--------------------------------------------

	f.close()

	print 'Successfully copied', file_size, 'bytes of data.'

if __name__ == "__main__":
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


