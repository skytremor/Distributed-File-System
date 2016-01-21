###############################################################################
#
# Filename: meta-data.py
# Author: Christian Rodriguez and Jose R. Ortiz
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
#

from mds_db import *
from Packet import *
import sys
import SocketServer

def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)


class MetadataTCPHandler(SocketServer.BaseRequestHandler):

	def handle_reg(self, db, p):
		"""Register a new client to the DFS  ACK if successfully REGISTERED
			NAK if problem, DUP if the IP and port already registered
		"""

		try:
			if db.AddDataNode(p.getAddr(), p.getPort()) != 0:
				self.request.sendall("ACK") 
			else:
				self.request.sendall("DUP")
		except:
			self.request.sendall("NAK")

	def handle_list(self, db):
		"""Get the file list from the database and send list to client"""
		try:
			# Fill code here
			packet = Packet()
			packet.BuildListResponse(db.GetFiles())
			self.request.sendall(packet.getEncodedPacket())
		except:
			self.request.sendall("NAK")	

	def handle_put(self, db, p):
		"""Insert new file into the database and send data nodes to save
		   the file.
		"""
	       
		# Fill code
		info = p.getFileInfo() 
	
		if db.InsertFile(info[0], info[1]): #If file inserted, send data nodes.
			# Fill code
			p.BuildPutResponse(db.GetDataNodes()) #Building packet to get serialized one.
			self.request.sendall(p.getEncodedPacket())

		else:
			self.request.sendall("DUP")
	
	def handle_get(self, db, p):
		"""Check if file is in database and return list of
			server nodes that contain the file.
		"""

		# Fill code to get the file name from packet and then 
		# get the fsize and array of metadata server

		fsize, mArray = db.GetFileInode(p.getFileName())

		if fsize:
			# Fill code
			p.BuildGetResponse(mArray, fsize) #Building packet to encode and send
			self.request.sendall(p.getEncodedPacket())
			print 'Found file with size', fsize,'bytes, sending info'
		else:
			self.request.sendall("NFOUND")
			print 'File not found.'

	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Get file name and blocks from packet
		filename = p.getFileName()
		blocks   = p.getDataBlocks()
		# Add blocks to file inode
		db.AddBlockToInode(filename, blocks)
		
	def handle(self):

		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)
	
		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files
			# Fill code
			self.handle_list(db)
		
		elif cmd == "put":
			# Client asking for servers to put data
			# Fill code
			self.handle_put(db, p)

		elif cmd == "get":
			# Client asking for servers to get data
			# Fill code
			self.handle_get(db, p)

		elif cmd == "dblks":
			# Client sending data blocks for file
			# Fill code
			self.handle_blocks(db, p)


		db.Close()

if __name__ == "__main__":
    HOST, PORT = "localhost", 8000

    if len(sys.argv) > 1:
    	try:
    		PORT = int(sys.argv[1])
    	except:
    		usage()

    server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    print "The meta-data server has successfully started!"
    server.serve_forever()
