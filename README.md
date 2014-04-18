Raft Log Replication
====================
What is this?
--------------
Implementation of Raft log replication over the existing leader election system done in earlier work.
This work will complete the functioning of Raft Log Replication.
What is the basic idea?
-----------------------
The work that is done in this project was proposed by a PhD student of Stanford University in his paper
Raft: In Search of an Understandable Consensus Algorithm. That work made a new perspective to the 
consensus algorithm. The main crib against the famous consensus algorithm Paxos was that it is not easy
to understand. So Raft comes into the picture to make a easy to understand consensus algorithm. My goal
is to implement Raft from end to end so that several implementation related issues can be identified
and addressed.

How this package is coming into the picture?
--------------------------------------------
This is the final consolidated version of the work. The whole work was developed in several phases:

- Designing distributed KV store
- Cluster communication network
- Leader election by Replicator servers of Raft
- Log replication among servers and apply the same to state machine


What is usefullness of this package?
------------------------------------
This will enable, distributed servers to achieve consensus. It has very easy to use interface. I tried
to design it in a modularized way.Due to the modularized nature several parts can be used or improved 
as per need.

How to use it?
-------------
*Please refer to general guidelines for importing a github package. Import this package into your workspace*

- There are two configuration file
- One is for replicator servers and another is for distributed KV servers and clients.
- Write the config file in the expected manner( described below)
- cd to the folder allcodes/replicator and run the command *go build*
- cd to the folder allcodes and run the command *go build ReplicatorMain.go*
- come back to the parent folder of raft
- Run the go test to ensure everything is on place.
- Make your required changes in the config and use for your purpose. BINGO.

What is the config file format?
------------------
Configuration file need to be in json

Below is a description  of configuration file for replicator interface.
	
	{
	"selfHandle":"tcp://127.0.0.1:", = Url of self

        "peersPid": "0,1,2,3,4",  = Pid of the peers
	
        "peersHandle":"tcp://127.0.0.1:", = This is handle for peers. If the servers are on different machine then this need to be modified, however that support though not a complex one, but not yet developed

        "startAddress":5000, =  This will be clubbed with the handle and will add with pid to make a unique network endpoint

        "startMsgId":1000, = Msg Id is kept in the envelope, this value is currently not used
 
        "timeoutMin":1500, = Minimum timeout for election

        "timeoutRand":1000,  = Time out for election is calculated by timeOutMin+ Random number between 0 to timeOutRand

        "outputFolder":"/tmp/",  = Where all outputs will be put including DB and Debug Logs
	
        "debugLogFile":"DEBUG_REPL_", = File name to save debug information

        "debugFlag":3,             =  Debug flag 1=print on terminal, 2= print+save in file 3= save in file, other values= nothing

        "levelDbForLog":"LOGDB_",  = Level db name which is used to save logs

        "termFileName":"TERM_"   = Persistant storage for Term
	}


Configuration file for connecting the KV servers and KV clients. This is useful because through this KV servers can forward some
request to the leader and KV clients cna connect with any KV server.

In this folder the file has one example on kvconfig.json
	{
	"selfHandle":"tcp://127.0.0.1:",

        "peersPid": "0,1,2,3,4,5",

        "peersHandle":"tcp://127.0.0.1:",

        "startAddress":6000,

        "startMsgId":100,

        "slaTimeout":5000,

        "totalServer":5,

        "waitTimeForLeader":1000,

        "outputFolder":"/tmp/",

        "kvDbName":"KVSDB_",

        "debugFlag":2,

        "debugFile":"KVDEBUG_"
	
	}

Fields with same name have same functionality as the config file for replicator servers. Some different fields are:

slaTimeOut : How much time put or get will wait before considering it as a failure
totalServer: The configuration will contain PID for both KVservers and KVclients. Starting with KVservers and once this totalServer
ends then clients PID will start. In the previous example, totalServer =5, means Client PID is 5, and servers (from peersPid) pid
are 0,1,2,3,4. 
startMsgId: Used with each request, initialized to starMsgId and increased monotonically


What are missing?
-----------------
the reviews. They are not to accomodate the requirement of this particular assignemnts 
rather based on the earlier reviewes. Some of the key changes are as below:

- As already mentioned peershandle need to be per peer basis, which is not there.
- Delete functionality is half baked, but not yet provided here.
- Test with multiple end client not yet done, will add soon.

What are the technology used?
----------------------------
- **Language:** GO 
- **Library:** zmq V4 was used
- **Paper Follwed:** Raft

Who are the people associated with this work?
---------------------------------------------
Implemented by **Kallol Dey**, M.Tech. II  CSE IIT Bombay,
Specification and guidance was provided by **Dr. Sriram Srinivasan**, Professor, CSE, IIT Bombay.


Current status of the project?
------------------------------
Developement completed. Can be upgraded in several way. Will continue this.
