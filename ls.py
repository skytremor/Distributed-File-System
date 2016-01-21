###############################################################################
#
# Filename: mds_db.py
# Author: Christian Rodriguez and Jose R. Ortiz
#
# Description:
# 	List client for the DFS
#

import socket

from Packet import *
import sys

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #Creating socket

	try:
		sock.connect((ip, port)) #Connecting socket
	except:
		print 'Could not connect to the server. Are you sure it is on and that you have the right port?'

	p = Packet() #Making a packet
	p.BuildListPacket() #Building packet
	
	try:
		sock.sendall(p.getEncodedPacket()) #Sending encoded packet
	except:
		print 'Could not send the encoded packet!'

	files = sock.recv(1024) #Receiving list of files

	p.DecodePacket(files) #Decoding Packet

	try:
		for files, size in p.getFileArray():
			print files, size, ' bytes'
	except:
		print 'Could not read list of files.'

if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)
