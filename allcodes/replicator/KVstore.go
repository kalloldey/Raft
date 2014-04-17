package replicator

import (
	"encoding/json"
	"fmt"
	"github.com/jmhodges/levigo"
	"os"
	"strconv"
	"time"
)

func (r KVserver) P(a string, b int, c int) {
	ck := r.DebugFlag
	FileLogging := r.DebugFile
	prt := string("[(" + strconv.Itoa(r.MyPid) + ") " + a + " || " + strconv.Itoa(b) + " :: " + strconv.Itoa(c) + " ]\n")
	if ck == 1 { //Only Print
		fmt.Print(prt)
	} else if ck == 2 { //Only Logging
		fl, _ := os.OpenFile(FileLogging, os.O_RDWR|os.O_APPEND, 0666)
		defer fl.Close()
		/*if(err !=nil){
		        fmt.Println("Some Error")
		}*/
		fl.WriteString(prt)
		fl.Sync()
	} else if ck == 3 { //Both Print + Logging
		fmt.Print(prt)
		fl, _ := os.OpenFile(FileLogging, os.O_RDWR|os.O_APPEND, 0666)
		defer fl.Close()
		/*if(err !=nil){
		        fmt.Println("Some Error")
		}*/
		fl.WriteString(prt)
		fl.Sync()
	}
}

var Conf struct {
	SelfHandle   string `json:"selfHandle"`
	PeersPid     string `json:"peersPid"`
	PeersHandle  string `json:"peersHandle"`
	StartAddr    int    `json:"startAddress"`
	StartMsgId   int    `json:"startMsgId"`
	SlaTimeOut   int    `json:"slaTimeout"`
	TotalServer  int    `json:"totalServer"`
	WaitTime     int    `json:"waitTimeForLeader"` //if I do not know the leader how much time I should wait before revert back error
	OutPutFolder string `json:"outputFolder"`
	DbForKV      string `json:"kvDbName"`
	DebugFlag    int    `json:"debugFlag"`
	DebugFile    string `json:"debugFile"`
}

type KVserver struct {
	MyPid       int
	ReplServer  *Replicator
	OuterComm   *Raftserver
	TotalServer int
	DataDbName  string //path-to-db
	DataDb      *levigo.DB
	SlaTimeOut  int
	WaitTime    int
	RepComplete chan *SMResp
	DebugFlag   int
	DebugFile   string
}

type SMCommand struct { //state machine command
	Command   string //put, get, delete
	Key       []byte
	Value     []byte
	UserId    int //required to return the result
	RequestId int
}

type SMResp struct {
	RequestId int
	Value     []byte
	Error     int
}

func (kvs *KVserver) GetCommitIndex() int { //Used in testing of Log
	return kvs.ReplServer.CommitIndex
}

func (kvs *KVserver) GetLogDBName() string {
	return kvs.ReplServer.LogDbName
}

/*
func (kvs *KVserver) LeadId() int {
	return kvs.ReplServer.WhoIsLeader()
}*/

//func ApplyToStateMachine(kvs *KVserver) {
func (kvs *KVserver) ApplyToStateMachine() {
	kvs.P("Apply to State Machine Routine", 0, 0)
	for {
		logdata := <-kvs.ReplServer.Inbox()
		switch logdata.Data.(type) {
		case SMCommand:
			{
				com, _ := logdata.Data.(SMCommand)
				kvs.P("Received msg from State machine ", com.UserId, com.RequestId)
				if com.Command == "PUT" || com.Command == "put" {
					wo := levigo.NewWriteOptions()
					err := kvs.DataDb.Put(wo, []byte(com.Key), []byte(com.Value))
					if kvs.ReplServer.WhoIsLeader() == kvs.MyPid {
						res := new(SMResp)
						res.RequestId = com.RequestId
						kvs.P("I am leader so will allow to send to client, req id", com.UserId, com.RequestId)
						if err != nil {
							res.Value = []byte("ERROR_IN_DB")
							res.Error = 1
						} else {
							res.Value = []byte("SUCCESS")
							res.Error = 0
						}
						kvs.RepComplete <- res
					}
				}
			} //end of case for SMC
		} //end of switch
	}
}

//func RunKVServer(kvs *KVserver) {
func (kvs *KVserver) RunKVServer() {
	kvs.P("RUN KV Server go routine", 0, 0)
	for {
		tmp := <-kvs.OuterComm.Inbox()
		switch tmp.Msg.(type) {
		case SMCommand:
			{
				lead := kvs.ReplServer.WhoIsLeader() //kvs.LeadId()
				if lead == -1 {
					kvs.P("Do not know leader ..so wait ", 0, 0)
					time.Sleep(time.Duration(kvs.WaitTime) * time.Millisecond)
					lead = kvs.ReplServer.WhoIsLeader()
				}
				cmd := tmp.Msg.(SMCommand)
				kvs.P("Received command Checking if I can process reqid,userid: ", cmd.RequestId, cmd.UserId)
				if lead == -1 { //still -1 ... can not do anything .. revert back error
					res := new(SMResp)
					res.RequestId = cmd.RequestId
					res.Value = []byte("I do not know leader")
					res.Error = 1
					kvs.OuterComm.Outbox() <- &Envelope{Pid: cmd.UserId, MsgId: 0, Msg: res}
					continue
				} else if lead != kvs.MyPid { //Forward to leader
					kvs.P("Msg forward to leader:", lead, 0)
					kvs.OuterComm.Outbox() <- &Envelope{Pid: lead, MsgId: 0, Msg: cmd}
					continue
				} //else I am leader
				kvs.P("Process command By Leader reqid,userid: ", cmd.RequestId, cmd.UserId)
				if cmd.Command == "GET" {
					//kvs.P("Get a Get request from client", 0, 0)
					ro := levigo.NewReadOptions()
					dot, err := kvs.DataDb.Get(ro, cmd.Key)
					res := new(SMResp)
					if err != nil {
						res.RequestId = cmd.RequestId
						res.Value = []byte("ERROR_IN_DB")
						res.Error = 1
					} else {
						res.RequestId = cmd.RequestId
						res.Value = dot
						res.Error = 0
					}
					kvs.OuterComm.Outbox() <- &Envelope{Pid: cmd.UserId, MsgId: 0, Msg: res}
				} else if cmd.Command == "PUT" {
					//kvs.P("Get a PUT request from client", 0, 0)
					kvs.ReplServer.Outbox() <- cmd
					looped := 0
					res := new(SMResp)
					res.RequestId = cmd.RequestId
					kvs.P("given to replicator interface .. wait for result: cmd.Userid,reqid", cmd.UserId, cmd.RequestId)
					tobreak := 1
					for {
						select {
						case <-time.After(time.Duration(kvs.SlaTimeOut-100) * time.Millisecond):
							{
								res.Value = []byte("TIMEOUT SLA")
								res.Error = 1
								kvs.P("Timeout and no response from state machine", 0, 0)
								tobreak = 1
							}
						case res = <-kvs.RepComplete:
							{
								kvs.P("Finally as a Leader Will send to client, req id", cmd.UserId, res.RequestId)
								kvs.P("Req id in res,cmd", res.RequestId, cmd.RequestId)
								if res.RequestId == cmd.RequestId {
									res.Value = nil
									res.Error = 0
									tobreak = 1
								} else if looped == 3 {
									res.Value = []byte("NoResponseFromLeader")
									kvs.P("Bad Response from Replicator", 0, 0)
									res.Error = 1
									tobreak = 1
								} else {
									looped++
									tobreak = 0
								}
							}
						}
						if tobreak == 1 {
							break
						}
					}
					kvs.P("Send Done to End Client", 0, 0)
					kvs.OuterComm.Outbox() <- &Envelope{Pid: cmd.UserId, MsgId: 0, Msg: res}
				} else if cmd.Command == "Lead" {
					res := new(SMResp)
					res.RequestId = cmd.RequestId
					res.Value = nil
					res.Error = kvs.ReplServer.WhoIsLeader() //Important error will contain the leader id
					kvs.OuterComm.Outbox() <- &Envelope{Pid: cmd.UserId, MsgId: 0, Msg: res}
				} else {
					res := new(SMResp)
					res.RequestId = cmd.RequestId
					res.Value = []byte("Mal Formed Req")
					res.Error = 1
					kvs.P("Mal Formed Req", 0, 0)
					kvs.OuterComm.Outbox() <- &Envelope{Pid: cmd.UserId, MsgId: 0, Msg: res}
				}

			} //end of case for SMCommand
		} //end of switch
	}

}

/*
Argument Description :
Replicator_FileName = Configuration file for the replicator framework used by the KV store
PidArg = Pid of this kv store. Remember this is going to be unique in both Configuring Replicator and Outer Communication
Commn_ConfigFile = Configuration file to make communication channel for the outer world client of key value service
*/

func KV_init(Replicator_FileName string, PidArg int, Commn_ConfigFile string) *KVserver {
	kvs := new(KVserver)

	configFile, err := os.Open(Commn_ConfigFile)
	if err != nil {
		fmt.Println("opening config file: ", Commn_ConfigFile, "..", err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&Conf); err != nil {
		fmt.Println("Error in parsing config file ", err.Error())
	}
	kvs.TotalServer = Conf.TotalServer
	kvs.MyPid = PidArg
	if kvs.MyPid >= kvs.TotalServer { //error		
		fmt.Println("[Config error]")
		return nil
	}
	kvs.OuterComm = New_DirectArg(kvs.MyPid, Conf.PeersPid, Conf.StartAddr, Conf.SelfHandle, Conf.PeersHandle)
	kvs.WaitTime = Conf.WaitTime
	kvs.SlaTimeOut = Conf.SlaTimeOut
	kvs.DebugFlag = Conf.DebugFlag
	kvs.DebugFile = string(Conf.OutPutFolder + Conf.DebugFile + strconv.Itoa(kvs.MyPid))
	os.Create(kvs.DebugFile)
	kvs.ReplServer = GetNew(Replicator_FileName, PidArg)
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	kvs.DataDbName = string(Conf.OutPutFolder + Conf.DbForKV + strconv.Itoa(kvs.MyPid))
	kvs.DataDb, _ = levigo.Open(kvs.DataDbName, opts)
	/*	if err!=nil{
	        fmt.Println("Error in creating DB for Data")
	}*/
	kvs.RepComplete = make(chan *SMResp, 100)
	kvs.P("KVS END : kvs slatime, kvs waittime", kvs.SlaTimeOut, kvs.WaitTime)
	go kvs.ApplyToStateMachine()
	go kvs.RunKVServer()
	return kvs
}
