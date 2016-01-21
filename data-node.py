###############################################################################
#
# Filename: data-node.py
# Authors: Christian Rodriguez and Jose R. Ortiz
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	try:
		sock.connect((meta_ip, meta_port))
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			sp.BuildRegPacket(data_ip, data_port)
			sock.sendall(sp.getEncodedPacket())
			response = sock.recv(1024)

			if response == "DUP":
				print "Duplicate Registration"

		 	if response == "NAK":
				print "Registratation ERROR"
	except:
		print 'ERROR: Unable to connect to server!\nPlease make sure the meta-data server is online.'
		sys.exit(0)

	finally:
		sock.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""
		recv_data = ""
		data_block_size = 0
		fname, fsize = p.getFileInfo()

		self.request.send("OK")

		# Generates an unique block id.
		blockid = str(uuid.uuid1())

		# Open the file for the new data block.
		saving_path = DATA_PATH+"/"+blockid
		f = open(saving_path, 'wb')

		# Receive the data block size first.
		data_block_size = self.request.recv(1024)

		print "This node must recieve", data_block_size, "bytes worth of data!"
		self.request.send("OK")

		ready_to_receive_data = self.request.recv(1024)
		try:
			self.request.send("OKSENDTHEDATA")
		except:
			print "Could not prepare for sending data\n\n\n"

		while (len(recv_data) < int(data_block_size)):
			bdata = self.request.recv(1024)
			recv_data = recv_data + bdata
			print 'Successfully received ', len(recv_data), ' bytes worth of data.'
		self.request.send("OK")
		
		#Write stored data to file
		f.write(recv_data)
		f.close()
		ack = self.request.recv(1024) #Ensures one send per recv

		#Send block id back
		self.request.sendall(blockid)
		self.request.close()

	def handle_get(self, p):
		
		# Get the block id from the packet
		b_id = p.getBlockID()

		# Read the file with the block id data
		saving_path = DATA_PATH+"/"+b_id
		f = open(saving_path, 'rb')
		fdata = f.read()
		f.close()
		
		dsize = len(fdata) #Data size

		# Retrieve and send data size to copy client

		self.request.sendall(str(dsize))
		ack1 = self.request.recv(1024) #One recv per send

		# Send it back to the copy client.
		self.request.sendall(fdata)
		ack2 = self.request.recv(1024) #One recv per send

		print 'Data node has sent all of it\'s data to copy!'
		self.request.close()

	def handle(self):
		msg = self.request.recv(1024) #From copyclient
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":	#Putting data
			self.handle_put(p)

		elif cmd == "get":	#Getting data
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 3:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()

	except:
		usage()


	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
 	server.serve_forever()
