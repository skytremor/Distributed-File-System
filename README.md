#Distributed File System

The purpose of this project is to implement the main components of a file system by implementing a simple, yet functional, distributed file system (DFS). The project involves 4 or more interacting processes. A metadata server, one or more data servers, a program that lists present files and a copy client. 

Components:
----------------------------------------------------------
The metadata server manages all requests related to the database. When the other processes need any information stored in the database, they must go through the server to acquire it.

The data servers are in charge of writing data blocks after they are received. They also send data from file when requested. Their existence is kept in the database by the metadata server.

The list client displays a list of files present in the system by asking the metaserver the names and sizes of the files.

This copy client manages the copying to and from the file system. It is the most complex. This process is in constant communication with the meta data server and specially the data nodes. Everytime it connects to a data node, it sends a 1024 byte block until the whole file is evenly distributed among the online data servers. Once the user asks to copy a file from the database, it does the whole process in reverse. It receives 1024 byte blocks and reconstructs the file in the desired path, specified by the user.

Instructions:
----------------------------------------------------------
The first step is to create a database with the script createdb.py in order to have the necessary structure to store the information needed for the DFS to funtion. 
    
    To create the database, simply type, (in a terminal window and DFS path):

        0. python createdb.py

Once the database is created, the metadata server can be started. It is important to note that all the other components depend on the meta server, so it is crucial that the metadata server be started first with the following instruction (the port is optional):

        1. python meta-data.py <port, default=8000> 

After the meta data server is up, you should be able to see a message that says, "The meta-data server has successfully started!".
Indicating the start up was successful.

-------------------------
The second step is to start the data node servers (as many as desired). It is important to note that each data node server must be run in a separate terminal tab, as they will be running simultaneously. The steps to do so is the same for every data node server.

        2. python data-node.py <server address> <port> <data path> <metadata port,default=8000>

            Arguments as following:
            - server address can be 127.0.0.1 (localhost)
            - try to choose a port from 1024 onwards and different from the meta-data server (8000 or custom)
            - data path to store the datablocks
            - choose the same port used by the meta-data server
-------------------------
The third step is to start the copy client and copy a file INTO the system, (on a terminal window different from the previous steps) as following:

        3. python copy.py <source file> <server address>:<port>:<dfs file path>

            Arguments as following:    
            - path and name of file to be copied
            - server adress can be 127.0.0.1 (localhost)
            - choose the same port used by the meta-data server
            - path or name that the file will be saved under in the system database
-------------------------
The fourth step is to start the copy client and copy a file FROM the system as following (can be the same terminal window):
        
        4. python copy.py <source file> <server>:<port>:<dfs file path>
        
            Arguments as following:
            - same as before
            - same as before
            - name given on previous command to file (see fourth argument of previous command)
            - destination file of the newly copied file
-------------------------
The fifth step is to examine the contents of the system:

        5. python ls.py <server>:<port, default=8000>
           
           Arguments as following:
            - server is localhost 
            - same as before
-------------------------
The sixth step is to do copy to, copy from or examine the contents of the system whenever you want.

        6. ??????

Warning: Do not copy something from the filesystem if it is empty. Why would you even do that? Follow these instructions and there should be no problem.
----------------------------------------------------------
