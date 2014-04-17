package replicator

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/jmhodges/levigo"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const COMSTR string = "##COMMIT_INDEX_SAVE##"

func (r *Replicator) P(a string, b int, c int) {
	ck := r.DebugFlag
	prt := string("[(" + strconv.Itoa(r.MyPid) + ") " + a + " || " + strconv.Itoa(b) + " :: " + strconv.Itoa(c) + " ]\n")
	if ck == 1 { //Only Print
		//fmt.Println("[(", r.MyPid, ") ", a, " || ", b, " :: ", c, " ]")
		fmt.Print(prt)
	} else if ck == 2 { //Only Logging
		fl, _ := os.OpenFile(r.FileLogging, os.O_RDWR|os.O_APPEND, 0666)
		defer fl.Close()
		/*if(err !=nil){
		        		fmt.Println("Some Error")
			        }*/
		fl.WriteString(prt)
		fl.Sync()
	} else if ck == 3 { //Both Print + Logging
		fmt.Print(prt)
		fl, _ := os.OpenFile(r.FileLogging, os.O_RDWR|os.O_APPEND, 0666)
		defer fl.Close()
		/*if(err !=nil){
		        		fmt.Println("Some Error")
			        }*/
		fl.WriteString(prt)
		fl.Sync()
	}
}

type RequestVote struct {
	Term        int
	CandidateId int
	LastLogNr   int
	LastLogTerm int
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogData      *LogItem
	LeaderCommit int
}

type AEResponse struct {
	Term         int
	SenderId     int
	PrevLogIndex int  //for which log index this response is meant for
	Type         int  //1: HBM's response, 2:AELog's response
	Accept       bool //will be useful for log compaction .. accept will be false on that
}

type LogItem struct {
	Index int //Log Index
	Term  int //Term in which this log is used
	Data  interface{}
}

type Raft interface {
	Term() int
	IsLeader() bool
	WhoIsLeader() int
	Detach()
	Attach()
	IsDetached() bool
	Outbox() chan interface{}
	Inbox() chan *LogItem
}

type Replicator struct {
	MyPid          int
	LeaderFlag     int //0: I am follower, 1: I am candidate, 2: I am leader
	CurrentTerm    int
	TotalPeer      int
	PidOfLeader    int //-1 when do not know
	Detached       int //set to 1 to detach this server from others ...
	BackServer     *Raftserver
	TimeOutMin     int
	TimeOutRand    int
	HBRecChan      chan int //Channel to indicate heartbeat received so no need of election
	VoteReceived   int
	BallotReceived int
	Locker         *sync.Mutex
	LeadLock       *sync.Mutex
	LinkPub        string        //link of the publisher
	Publisher      *zmq.Socket   //publisher socket...
	InboxChan      chan *LogItem //
	OutboxChan     chan interface{}
	CommitIndex    int
	LastLogNr      int
	VolatileStore  []LogItem
	PeersLogIndex  []int
	OutboxInProg   chan int //used to stop processing request on outbox before the earlier one get finished
	SendAHBM       chan int
	TempChanOB     chan *AEResponse
	FileLogging    string
	LogDbName      string
	LogDb          *levigo.DB
	DebugFlag      int
	TermFile       string
}

var sett struct {
	SelfHandle       string `json:"selfHandle"`
	PeersPid         string `json:"peersPid"`
	PeersHandle      string `json:"peersHandle"`
	StartAddr        int    `json:"startAddress"`
	StartMsgId       int    `json:"startMsgId"`
	TimeOutMin       int    `json:"timeoutMin"`  //Minimum timeout length
	TimeOutRand      int    `json:"timeoutRand"` //One value in between 0 to TimeOutRand will be choosen and added to TimtOutMin
	OutPutFolder     string `json:"outputFolder"`
	DebugLogFileName string `json:"debugLogFile"`
	DebugFlag        int    `json:"debugFlag"`
	LogDbName        string `json:"levelDbForLog"`
	TermFileName     string `json:"termFileName"`
}

func (r Replicator) Outbox() chan interface{} {
	return r.OutboxChan
}

func (r Replicator) Inbox() chan *LogItem {
	return r.InboxChan
}

func (r Replicator) WhoIsLeader() int {
	return r.PidOfLeader
}

func (r Replicator) GetLink() string {
	return r.LinkPub
}

func (r Replicator) Term() int {
	return r.CurrentTerm
}
func (r Replicator) IsLeader() bool {
	if r.LeaderFlag == 2 {
		return true
	}
	return false
}

func (r *Replicator) Detach() {
	r.P("I am inside Detach call", 0, 0)
	r.Detached = 1
}

func (r *Replicator) Attach() {
	r.P("I am inside Attach call", 0, 0)
	r.CurrentTerm = GetTerm(r)
	r.Detached = 0
}

func (r Replicator) IsDetached() bool {
	if r.Detached == 1 {
		return true
	}
	return false
}

func (r *Replicator) PutCom() {
	wo := levigo.NewWriteOptions()
	r.LogDb.Put(wo, []byte(COMSTR), []byte(strconv.Itoa(r.CommitIndex)))
}

func (r *Replicator) GetCom() int {
	ro := levigo.NewReadOptions()
	dot, err := r.LogDb.Get(ro, []byte(COMSTR))
	if err != nil {
		r.P("Commit index not found in DB", 0, 0)
		return -1
	} else {
		v, e := strconv.Atoi(string(dot))
		if e != nil {
			r.P("Commit Index malformed: so -1", 0, 0)
			return -1
		} else {
			r.P("Givign Commit Index : ", v, 0)
			return v
		}
	}
}

func (r *Replicator) PutLog(lognr int, logit *LogItem) int {

	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	gob.Register(LogItem{})
	encCache.Encode(logit)
	sendmsg := mCache.Bytes()
	wo := levigo.NewWriteOptions()
	err := r.LogDb.Put(wo, []byte(strconv.Itoa(lognr)), sendmsg)
	if err != nil {
		r.P("Error while putting log in level db, lognr:", lognr, 0)
		return 1
	}
	return 0
}

func (r *Replicator) GetLog(lognr int) *LogItem {
	ro := levigo.NewReadOptions()
	dot, err := r.LogDb.Get(ro, []byte(strconv.Itoa(lognr)))
	if err != nil {
		return nil
	} else {
		var ret LogItem
		pCache := bytes.NewBuffer(dot)
		decCache := gob.NewDecoder(pCache)
		decCache.Decode(&ret)
		return &ret
	}
}

func (r *Replicator) DelLog(lognr int) int {
	wo := levigo.NewWriteOptions()
	err := r.LogDb.Delete(wo, []byte(strconv.Itoa(lognr)))
	if err != nil {
		return 1
	} else {
		return 0
	}
}

func GetTerm(rp *Replicator) int {
	tmflname := rp.TermFile
	termfile, err := os.Open(tmflname)
	myTerm := 0
	if err != nil { //If no file exist then create one
		myTerm = 0
		termfile, err = os.Create(tmflname)
		//              fmt.Println("If part")
		d3 := []byte(string(strconv.Itoa(myTerm) + "\n"))
		ioutil.WriteFile(tmflname, d3, 0644)
	}
	defer termfile.Close()
	bt := make([]byte, 10)
	termfile.Read(bt)
	lines := strings.Split(string(bt), "\n")
	kl := lines[0]
	//        fmt.Println("String read : ",kl)
	myTerm, err = strconv.Atoi(kl)
	if err != nil {
		fmt.Println("Err on conv")
	}
	return myTerm
} //OK

var mutex = &sync.Mutex{}

func SetTerm(rp *Replicator, newTerm int) int {
	mutex.Lock()
	tmflname := rp.TermFile
	termfile, err := os.Open(tmflname)
	if err != nil {
		termfile, err = os.Create(tmflname)
	}
	defer termfile.Close()
	rp.CurrentTerm = newTerm
	d3 := []byte(string(strconv.Itoa(newTerm) + "\n"))
	ioutil.WriteFile(tmflname, d3, 0644)
	mutex.Unlock()
	return newTerm
} //OK

func StartVote(rp *Replicator) {
	//Increase term by one
	if rp.LeaderFlag == 2 { //I am already leader .. no more election needed
		return
	}
	if rp.LeaderFlag == 1 { //I am candidate .. some election already in process .. let that finish
		//return    //In the purpose of some BUG remove ... 1304
	}
	if rp.Detached == 1 { //I am detached why tell me to start vote ???
		rp.P("I am detached", 0, 0)
		return
	}
	rp.LeaderFlag = 1 //convert to candidate state
	rp.PidOfLeader = -1
	voteForTerm := rp.CurrentTerm + 1
	rp.BallotReceived = 1
	SetTerm(rp, voteForTerm) //Save in file
	var tmpTerm int
	if rp.LastLogNr == -1 {
		tmpTerm = -1
	} else {
		//		tmplog := rp.VolatileStore[rp.LastLogNr]
		tmplog := rp.GetLog(rp.LastLogNr)
		if tmplog == nil {
			tmpTerm = -1
		} else {
			tmpTerm = tmplog.Term
		}
	}
	sm := RequestVote{Term: voteForTerm, CandidateId: rp.MyPid, LastLogNr: rp.LastLogNr, LastLogTerm: tmpTerm}
	rp.BackServer.Outbox() <- &Envelope{Pid: -1, MsgId: 4, Msg: sm}
}

func ElectionCommison(rp *Replicator) { //Detached case taken care by the StartVote .. so here can ignore
	for {
		/*	if rp.Detached == 1 {
				continue
			}
			if rp.LeaderFlag == 2 {
				continue
			}*/
		tmout := time.Duration(rp.TimeOutMin + rand.Intn(rp.TimeOutRand+rp.MyPid*2))
		select {
		case <-rp.HBRecChan:
			//HB received go for the next iteration of for loop ..
			//NO OP
			continue
		case <-time.After(tmout * time.Millisecond):
			StartVote(rp)
		}
	}
}

func PublishLead(rp *Replicator) {
	for {
		time.Sleep(time.Duration(600) * time.Millisecond) //I will send after 400 second .. make sure your loop probe after that
		if rp.IsLeader() == false {
			continue
		}
		//		fmt.Println("Publish Leader Msg")
		rp.Publisher.SendMessage(string("$ILEAD$"+strconv.Itoa(rp.MyPid)+"$INTERM$") + strconv.Itoa(rp.CurrentTerm))
	}
}

func SendHBM(rp *Replicator) { //when ever i am leader send Heart beat message  //Detached case taken care
	for {
		select {
		case <-time.After(time.Duration(rp.TimeOutMin-200) * time.Millisecond):
		case <-rp.SendAHBM:
		}
		//		time.Sleep(time.Duration(120) * time.Millisecond)
		if rp.IsLeader() == false {
			continue
		}
		if rp.Detached == 1 {
			rp.P("I am detached", 0, 0)
			time.Sleep(time.Duration(100) * time.Millisecond) //if I am detached .. so use this to slow down checking
			continue
		}
		//		sm := string("HEARTBEAT$" + strconv.Itoa(rp.MyPid) + "$" + strconv.Itoa(rp.CurrentTerm))
		//Make a "Empty Append RPC" for each server and send the same

		//		rp.Locker.Lock()
		rp.P("Send HBM for term, commit index ", rp.CurrentTerm, rp.CommitIndex)
		for vari := 0; vari < rp.TotalPeer; vari++ {
			if vari == rp.MyPid {
				continue
			}
			prev_log_term := -1
			if rp.PeersLogIndex[vari] == -1 {
				prev_log_term = -1
			} else {
				//	itm := rp.VolatileStore[rp.PeersLogIndex[vari]]
				itm := rp.GetLog(rp.PeersLogIndex[vari])
				if itm == nil {
					prev_log_term = -1
				} else {
					prev_log_term = itm.Term
				}
			}
			sm := AppendEntries{Term: rp.CurrentTerm, LeaderId: rp.MyPid, PrevLogIndex: rp.PeersLogIndex[vari], PrevLogTerm: prev_log_term, LogData: nil, LeaderCommit: rp.CommitIndex}
			rp.BackServer.Outbox() <- &Envelope{Pid: vari, MsgId: 2, Msg: sm}
		}
	}
} //Done 2

//Vote for me msg pattern:    VOTEME$<MyPid>$ForTheTerm$
//Reply with Deny Vote msg pattern:   VOTEDENY $ Pid of server $ For the term $
//Reply with Grant Vote msg pattern:   VOTEGRANT $Pid of server $ For the term $
//Leader Heartbeat msg pattern: HEARTBEAT$Pid of the leader $for the term $

func TelecomMinistry(rp *Replicator) {
	for {
		rec := <-rp.BackServer.Inbox()
		if rp.Detached == 1 {
			rp.P("I am detached", 0, 0)
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}
		switch rec.Msg.(type) {
		case AppendEntries:
			{
				obj, _ := rec.Msg.(AppendEntries)
				if rp.CurrentTerm > obj.Term {
					rp.P("FATAL PROBLEM .. current term is more than leader", rp.CurrentTerm, obj.Term)
					//THIS IS NOT A HBM
					continue
					//Not possible error case
					/*This only can happen if a node goes down when it was leader and after sometime
						  came back and continue to send hbm .. however this will be automatically taken
					          care by election process and term updation on vote deny
					*/
				}
				/* 3 types .. Empty HBM type AE from Leader,
				Request to Append Entry from Leader,
				Response to an earlier Append Entry*/
				if obj.LogData == nil { //a HBM ..handle keeping the backward com
					//rp.P("APE is HBM", 0, 0)
					//obj.PrevLogIndex need to be checked with MY log and send response accordingly
					smF := AEResponse{Term: rp.CurrentTerm, SenderId: rp.MyPid, PrevLogIndex: obj.PrevLogIndex, Type: 1, Accept: false}
					smT := AEResponse{Term: rp.CurrentTerm, SenderId: rp.MyPid, PrevLogIndex: obj.PrevLogIndex, Type: 1, Accept: true}
					if rp.LastLogNr >= obj.PrevLogIndex {
						var tmpTerm int
						if obj.PrevLogIndex == -1 {
							tmpTerm = -1
						} else {
							//tmplog := rp.VolatileStore[obj.PrevLogIndex]
							tmplog := rp.GetLog(obj.PrevLogIndex)
							if tmplog == nil {
								tmpTerm = -1
							} else {
								tmpTerm = tmplog.Term
							}
						}
						if tmpTerm == obj.PrevLogTerm {
							//	rp.P("I am here 334", 0, rp.MyPid)
							if obj.LeaderCommit > rp.CommitIndex {
								if rp.LastLogNr == obj.PrevLogIndex && obj.LeaderCommit == rp.LastLogNr {
									//	rp.P("I am here 334 A", obj.LeaderCommit, rp.MyPid)
									//This is command to commit a uptodate message
									rp.CommitIndex = obj.LeaderCommit
									rp.PutCom()
									//cmtlog := rp.VolatileStore[rp.CommitIndex]
									cmtlog := rp.GetLog(rp.CommitIndex)
									rp.InboxChan <- cmtlog
									sms, _ := cmtlog.Data.(string)
									rp.P(string("commited msg: "+sms+"| cmt index: "), rp.CommitIndex, 0)
								} else { //	rp.LastLogNr = obj.PrevLogIndex
									rp.P("I am here 463 B", 0, rp.MyPid)
									if rp.CommitIndex < obj.PrevLogIndex {
										rp.P("I am here 465 C", 0, rp.MyPid)
										rp.LastLogNr = obj.PrevLogIndex //shifted UP
										rp.CommitIndex = rp.LastLogNr
										rp.PutCom()
										//cmtlog := rp.VolatileStore[rp.CommitIndex]
										cmtlog := rp.GetLog(rp.CommitIndex)
										rp.InboxChan <- cmtlog //can comment this ..
									}
								}
							} //else is not possible
							rp.BackServer.Outbox() <- &Envelope{Pid: obj.LeaderId, MsgId: 3, Msg: smT}
						} else {
							//rp.P("I am here 348", 0, 0)
							if rp.LastLogNr != -1 {
								rp.LastLogNr = obj.PrevLogIndex - 1
							}
							rp.BackServer.Outbox() <- &Envelope{Pid: obj.LeaderId, MsgId: 3, Msg: smF}
						}
					} else {
						rp.P("Send False to reply HBM my log nr less,last log,commit index", rp.LastLogNr, rp.CommitIndex)
						rp.BackServer.Outbox() <- &Envelope{Pid: obj.LeaderId, MsgId: 3, Msg: smF}
					}
				} else { //not a HBM handle this
					smF := AEResponse{Term: rp.CurrentTerm, SenderId: rp.MyPid, PrevLogIndex: obj.PrevLogIndex, Type: 2, Accept: false}
					smT := AEResponse{Term: rp.CurrentTerm, SenderId: rp.MyPid, PrevLogIndex: obj.PrevLogIndex, Type: 2, Accept: true}
					if rp.LastLogNr >= obj.PrevLogIndex {
						var tmpTerm int
						if obj.PrevLogIndex == -1 {
							tmpTerm = -1
						} else {
							//tmplog := rp.VolatileStore[obj.PrevLogIndex]
							tmplog := rp.GetLog(obj.PrevLogIndex)
							if tmplog == nil {
								tmpTerm = -1
							} else {
								tmpTerm = tmplog.Term
							}
						}
						if tmpTerm == obj.PrevLogTerm {
							if rp.CommitIndex <= obj.PrevLogIndex {
								//deletion req here ....
								prevLogNr := rp.LastLogNr
								rp.LastLogNr = obj.PrevLogIndex + 1
								//delete all entry from rp.LastLogNr+1 to prevLogNr
								for pg := rp.LastLogNr + 1; pg <= prevLogNr; pg++ {
									rp.DelLog(pg)
								}
								//rp.VolatileStore[rp.LastLogNr] = *obj.LogData
								rp.PutLog(rp.LastLogNr, obj.LogData)
								rp.P("I logged an entry, my lognr:Leader commit:", rp.LastLogNr, obj.LeaderCommit)
								fmt.Println("Loged data is: ", obj.LogData)

								if obj.LeaderCommit > rp.CommitIndex {
									if obj.LeaderCommit > rp.LastLogNr {
										rp.CommitIndex = rp.LastLogNr
										rp.PutCom()
										rp.P("[A]Msg Directly Commited ||my commit index, last log", rp.CommitIndex, rp.LastLogNr)

										cmtlog := rp.GetLog(rp.CommitIndex)
										rp.InboxChan <- cmtlog
									} else {
										rp.CommitIndex = obj.LeaderCommit
										rp.P("[B]Msg Directly Commited ||my commit index, last log", rp.CommitIndex, rp.LastLogNr)
										cmtlog := rp.GetLog(rp.CommitIndex)
										rp.InboxChan <- cmtlog
									}
								}
							}
							rp.BackServer.Outbox() <- &Envelope{Pid: obj.LeaderId, MsgId: 3, Msg: smT}
						} else {
							rp.BackServer.Outbox() <- &Envelope{Pid: obj.LeaderId, MsgId: 3, Msg: smF}
						}
					} else {
						rp.BackServer.Outbox() <- &Envelope{Pid: obj.LeaderId, MsgId: 3, Msg: smF}
					}
				}

				if obj.Term > rp.CurrentTerm {
					SetTerm(rp, obj.Term)
				}
				if obj.LogData == nil {
					rp.P("Got Pure HBM from leader: Prev log index, leader commit", obj.PrevLogIndex, obj.LeaderCommit)
				} else {

					rp.P("Got AE HBM from leader: Prev log index, leader commit", obj.PrevLogIndex, obj.LeaderCommit)
				}
				rp.HBRecChan <- 1
				rp.LeaderFlag = 0
				rp.PidOfLeader = obj.LeaderId
				continue
			}
		case AEResponse:
			{
				if rp.IsLeader() == false {
					continue
				}
				obj, _ := rec.Msg.(AEResponse)
				//if hbm's response .. see if mismatch in log nr.. then send those log to followers..
				//other response of append entries only come if I am leader .. forward them to the r.TempChanOB so that outbox can use
				if obj.Type == 1 { // response to HBM
					if !obj.Accept {
						rp.P("Get false reply for HBM", obj.SenderId, obj.PrevLogIndex)
						if rp.PeersLogIndex[obj.SenderId] >= 0 {
							if obj.PrevLogIndex == rp.PeersLogIndex[obj.SenderId] {
								rp.PeersLogIndex[obj.SenderId]--

								/*rp.PeersLogIndex[obj.SenderId] and obj.PrevLogIndex-1 both should be same at this point*/
								var prev_log_term int
								if rp.PeersLogIndex[obj.SenderId] == -1 {
									prev_log_term = -1
								} else {
									//prev_log := rp.VolatileStore[obj.PrevLogIndex-1]
									prev_log := rp.GetLog(rp.PeersLogIndex[obj.SenderId])
									if prev_log == nil {
										prev_log_term = -1
									} else {
										prev_log_term = prev_log.Term
									}
								}
								rp.P("Got a false msg from : and will send the prev term as", obj.SenderId, rp.PeersLogIndex[obj.SenderId])
								sm := AppendEntries{Term: rp.CurrentTerm, LeaderId: rp.MyPid, PrevLogIndex: rp.PeersLogIndex[obj.SenderId], PrevLogTerm: prev_log_term, LogData: nil, LeaderCommit: rp.CommitIndex}
								rp.BackServer.Outbox() <- &Envelope{Pid: obj.SenderId, MsgId: 3, Msg: sm}
							}
						}
					} else {
						if obj.PrevLogIndex < rp.CommitIndex {
							if obj.PrevLogIndex == rp.PeersLogIndex[obj.SenderId] {
								var prev_log_term int
								if obj.PrevLogIndex == -1 {
									prev_log_term = -1
								} else {
									//prev_log := rp.VolatileStore[obj.PrevLogIndex]
									prev_log := rp.GetLog(obj.PrevLogIndex)
									if prev_log == nil {
										prev_log_term = -1
									} else {
										prev_log_term = prev_log.Term
									}
								}
								//logit := rp.VolatileStore[obj.PrevLogIndex+1]
								logit := rp.GetLog(obj.PrevLogIndex + 1)
								smm := AppendEntries{Term: rp.CurrentTerm, LeaderId: rp.MyPid, PrevLogIndex: obj.PrevLogIndex, PrevLogTerm: prev_log_term, LogData: logit, LeaderCommit: rp.CommitIndex}
								if obj.SenderId == 3 {
									rp.P("after a while got true from 0 hbm will send the log entry of ", obj.PrevLogIndex, 0)
								}
								rp.BackServer.Outbox() <- &Envelope{Pid: obj.SenderId, MsgId: 3, Msg: smm}
							} else if obj.PrevLogIndex > rp.PeersLogIndex[obj.SenderId] {
								rp.PeersLogIndex[obj.SenderId] = obj.PrevLogIndex
							}
						}
					}

				} else { //response to AE
					if !obj.Accept {
						rp.P("Get false reply for AEntry", obj.SenderId, obj.PrevLogIndex)
						if obj.PrevLogIndex >= 0 {
							if obj.PrevLogIndex == rp.PeersLogIndex[obj.SenderId] {
								//bcoz when u send the recent AE that will also come back as false
								rp.PeersLogIndex[obj.SenderId]--

								/*rp.PeersLogIndex[obj.SenderId] and obj.PrevLogIndex-1 both should be same at this point*/
								var prev_log_term int
								if rp.PeersLogIndex[obj.SenderId] == -1 {
									prev_log_term = -1
								} else {
									//prev_log := rp.VolatileStore[rp.PeersLogIndex[obj.SenderId]]
									prev_log := rp.GetLog(rp.PeersLogIndex[obj.SenderId])
									if prev_log == nil {
										prev_log_term = -1
									} else {
										prev_log_term = prev_log.Term
									}
								}
								sm := AppendEntries{Term: rp.CurrentTerm, LeaderId: rp.MyPid, PrevLogIndex: rp.PeersLogIndex[obj.SenderId], PrevLogTerm: prev_log_term, LogData: nil, LeaderCommit: rp.CommitIndex}
								rp.BackServer.Outbox() <- &Envelope{Pid: obj.SenderId, MsgId: 3, Msg: sm}
							}
						}
					} else {
						/*Accept of 2 type .. one catching up the commited log another answer to update lagging commited logs in follower */
						if obj.PrevLogIndex == rp.CommitIndex {
							rp.PeersLogIndex[obj.SenderId]++
							rp.TempChanOB <- &obj
						} else if obj.PrevLogIndex+1 < rp.CommitIndex {
							//send the next commited log
							if obj.PrevLogIndex == rp.PeersLogIndex[obj.SenderId] {
								rp.PeersLogIndex[obj.SenderId]++ //= obj.PrevLogIndex+1

								//prev_log := rp.VolatileStore[rp.PeersLogIndex[obj.SenderId]]
								prev_log := rp.GetLog(rp.PeersLogIndex[obj.SenderId])
								prev_log_term := prev_log.Term
								//logit := rp.VolatileStore[rp.PeersLogIndex[obj.SenderId]+1]
								//logit := rp.GetLog(rp.PeersLogIndex[obj.SenderId] + 1)
								smm := AppendEntries{Term: rp.CurrentTerm, LeaderId: rp.MyPid, PrevLogIndex: rp.PeersLogIndex[obj.SenderId], PrevLogTerm: prev_log_term, LogData: nil, LeaderCommit: rp.CommitIndex}
								rp.BackServer.Outbox() <- &Envelope{Pid: obj.SenderId, MsgId: 3, Msg: smm}
							}
						} else if obj.PrevLogIndex+1 == rp.CommitIndex {
							if obj.PrevLogIndex == rp.PeersLogIndex[obj.SenderId] {
								rp.PeersLogIndex[obj.SenderId]++
							}
							//no need to send to channel .. as it already get past it
						} else {
							//ERROR Condition
							rp.P("Error ##############################", 0, 0)
						}
					}
				} // response to AE/HBM if else end
			} //AE end
		case RequestVote:
			{
				obj, _ := rec.Msg.(RequestVote)
				serverPid := obj.CandidateId
				forTerm := obj.Term
				//	rp.P("Got Request Vote ",serverPid,forTerm)
				if rp.CurrentTerm < forTerm {
					if rp.LastLogNr <= obj.LastLogNr {
						//covert own to follower state rp.
						SetTerm(rp, forTerm)
						denmsg := string("VOTEGRANT$" + strconv.Itoa(rp.MyPid) + "$" + strconv.Itoa(forTerm))
						//rp.Locker.Lock()
						rp.LeaderFlag = 0
						//rp.P("I am GIVING vote to, in term", serverPid, forTerm)
						rp.BackServer.Outbox() <- &Envelope{Pid: serverPid, MsgId: 0, Msg: denmsg}
						//                                      rp.Locker.Unlock()
					} else {
						//rp.P("Votedenying Path 1",0,0)
						//SO THIS IS THE VILLAIN PATH
						denmsg := string("VOTEDENY$" + strconv.Itoa(rp.MyPid) + "$" + strconv.Itoa(rp.CurrentTerm))
						//rp.Locker.Lock()
						//rp.P("I am DENYing vote to,term ", serverPid, forTerm)
						rp.BackServer.Outbox() <- &Envelope{Pid: serverPid, MsgId: 0, Msg: denmsg}
						//rp.Locker.Unlock()
						SetTerm(rp, forTerm)
						if rp.LeaderFlag == 2 {
							rp.LeaderFlag = 0 //I step down as candidate and again start fresh elec
							//SetTerm(rp, forTerm)
							StartVote(rp)
						}
					}
				} else {
					//	rp.P("Vote denying part 2",0,0)
					denmsg := string("VOTEDENY$" + strconv.Itoa(rp.MyPid) + "$" + strconv.Itoa(rp.CurrentTerm))
					//rp.Locker.Lock()
					//rp.P("I am DENYing vote to,term ", serverPid, forTerm)
					rp.BackServer.Outbox() <- &Envelope{Pid: serverPid, MsgId: 0, Msg: denmsg}
					//rp.Locker.Unlock()
				}

			}
		case string: //will convert this also to RequestVoteRPC
			{ //Handling Type String Mostly Of Voting Purpose
				strs, _ := rec.Msg.(string)
				temp := strings.Split(strs, "$")
				//		fmt.Println("Msg found:  ",rec.Msg)
				//serverPid, _ := strconv.Atoi(temp[1])
				//		fmt.Println("Temp1: ",temp[1]," | serverPid: ",serverPid)
				forTerm, _ := strconv.Atoi(temp[2])

				switch temp[0] {
				case "VOTEDENY":
					{
						if forTerm > rp.CurrentTerm {
							//	rp.P("Get deny msg from,term", serverPid, forTerm)
							rp.LeaderFlag = 0 //become follower again as big term is present in somewhere
							SetTerm(rp, forTerm)
							//Doubt to clear: Will VOTEDENY will get the current update term from the responder server ??
						}
					}
				case "VOTEME":
					{ /*
							serverPid, _ := strconv.Atoi(temp[1])
							forTerm, _ := strconv.Atoi(temp[2])
							if rp.CurrentTerm < forTerm {
								//covert own to follower state rp.
								SetTerm(rp, forTerm)
								denmsg := string("VOTEGRANT$" + strconv.Itoa(rp.MyPid) + "$" + strconv.Itoa(forTerm))
								//					rp.Locker.Lock()
								rp.LeaderFlag = 0
								//rp.P("I am GIVING vote to, in term", serverPid, forTerm)
								rp.BackServer.Outbox() <- &Envelope{Pid: serverPid, MsgId: 0, Msg: denmsg}
								//					rp.Locker.Unlock()
							} else {
								denmsg := string("VOTEDENY$" + strconv.Itoa(rp.MyPid) + "$" + strconv.Itoa(forTerm))
								//					rp.Locker.Lock()
								//rp.P("I am DENYing vote to,term ", serverPid, forTerm)
								rp.BackServer.Outbox() <- &Envelope{Pid: serverPid, MsgId: 0, Msg: denmsg}
								//					rp.Locker.Unlock()
							}*/
					}
				case "VOTEGRANT":
					{
						if rp.LeaderFlag == 1 && forTerm == rp.CurrentTerm {
							rp.VoteReceived++
							//	rp.P("Received VOTE,from in term", serverPid, forTerm)
							if rp.VoteReceived >= ((rp.TotalPeer / 2) + (rp.TotalPeer % 2)) { //self become leader
								if rp.OutboxInProg == nil {
									rp.OutboxInProg = make(chan int, 10)
								} else {
									close(rp.OutboxInProg)
									rp.OutboxInProg = make(chan int, 10)
								}
								rp.LeaderFlag = 2
								//	rp.P("I am now leader,total vote rec,term", rp.VoteReceived, forTerm)
								rp.PidOfLeader = rp.MyPid
								for z := 0; z < rp.TotalPeer; z++ {
									rp.PeersLogIndex[z] = rp.LastLogNr
								}
								rp.OutboxInProg <- 1
								rp.SendAHBM <- 1
							}
						}
					}
				default:
					{
						//NO OP
					}
				}
			} //String Type Handling Switch Ends here
		default:
			{
				if rp.MyPid == 3 {
					rp.P("This is a not matched type", 0, 0)
				}
			}
		} //switch based on type ...ends here
	} //for loop ends here
}

func GetNew(FileName string, PidArg int) *Replicator {
	//File read starts ....
	//	fmt.Println("Start of GetNew")
	repl := new(Replicator)
	configFile, err := os.Open(FileName)
	if err != nil {
		fmt.Println("opening config file: ", FileName, "..", err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&sett); err != nil {
		fmt.Println("Error in parsing config file ", err.Error())
	}
	repl.MyPid = PidArg
	fmt.Println("Debug file", sett.DebugLogFileName)
	repl.FileLogging = string(sett.OutPutFolder + sett.DebugLogFileName + strconv.Itoa(repl.MyPid))
	repl.DebugFlag = sett.DebugFlag
	os.Create(repl.FileLogging)
	repl.LeaderFlag = 0
	repl.PidOfLeader = -1
	//termFile, err := os.Open(string("termfileDB" + strconv.Itoa(repl.MyPid))) //termfileDB1
	repl.HBRecChan = make(chan int, 10)
	repl.TermFile = string(sett.OutPutFolder + sett.TermFileName + strconv.Itoa(repl.MyPid))
	repl.P(repl.TermFile, 0, 0)
	repl.CurrentTerm = GetTerm(repl)
	repl.TimeOutMin = sett.TimeOutMin
	repl.TimeOutRand = sett.TimeOutRand
	repl.TotalPeer = len(strings.Split(sett.PeersPid, ","))
	repl.P("Total Peer : ", repl.TotalPeer, 0)
	repl.Detached = 0
	//	Pub Sub notifier
	repl.Publisher, _ = zmq.NewSocket(zmq.PUB)
	repl.LinkPub = string("tcp://localhost:" + strconv.Itoa(sett.StartAddr+1000+repl.MyPid)) //1000 more than the address for my inbox/outbox iface
	repl.Publisher.Bind("tcp://*:" + strconv.Itoa(sett.StartAddr+1000+repl.MyPid))
	//
	repl.Locker = &sync.Mutex{}
	repl.LeadLock = &sync.Mutex{}
	/*	New_DirectArg(self pid, "pid of peers in a comma separated string", Strat address of the port as int, Self handle in string, Peers handle in string)*/
	repl.BackServer = New_DirectArg(repl.MyPid, sett.PeersPid, sett.StartAddr, sett.SelfHandle, sett.PeersHandle)
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	repl.LogDbName = string(sett.OutPutFolder + sett.LogDbName + strconv.Itoa(repl.MyPid))
	repl.P(repl.LogDbName, 0, 0)
	var e error
	repl.LogDb, e = levigo.Open(repl.LogDbName, opts)
	if e != nil {
		repl.P("Levigo creates error: can not continue"+e.Error(), 0, 0)
		return nil
	}
	repl.InboxChan = make(chan *LogItem, 100)
	repl.OutboxChan = make(chan interface{}, 100)
	repl.TempChanOB = make(chan *AEResponse, 100)
	repl.SendAHBM = make(chan int, 10)
	repl.CommitIndex = repl.GetCom()  // <-- If Needed to read from persistant storage, use this simply
	repl.LastLogNr = repl.CommitIndex //no need to read from persistant storage
	repl.VolatileStore = make([]LogItem, 100)
	repl.PeersLogIndex = make([]int, repl.TotalPeer+1)
	for mc := 0; mc < repl.TotalPeer; mc++ {
		repl.PeersLogIndex[mc] = repl.LastLogNr
	}
	go TelecomMinistry(repl)
	go ElectionCommison(repl)
	go SendHBM(repl)
	go PublishLead(repl)
	go OutboxRoutine(repl)
	repl.P("Initiate: Change 17040456", 0, 0)
	repl.Publisher.SendMessage(string("ISTARTED" + strconv.Itoa(repl.MyPid)))
	return repl
}

func OutboxRoutine(r *Replicator) {
	for {
		var item interface{}
		select { //just to drop all the packets that are send to outbox when I am NONLEADER
		case item = <-r.OutboxChan:
			if r.IsLeader() != true {
				fmt.Println("My Pid: ", r.MyPid)
				r.P("*************in outbox... I am Not leader so drop the msg ********", 0, 0)
				continue
			} else {
				<-r.OutboxInProg
				r.P("Out of wait will proceed to send", 0, 0)
			}
			//		case <-time.After(40 * time.Millisecond):
			//			continue
		}
		r.P("I am leader and I got my msg in outbox", 0, 0)
		//make volatile entry in own area
		logit := LogItem{Index: r.LastLogNr + 1, Term: r.CurrentTerm, Data: item}
		var prev_log_term int
		if r.LastLogNr == -1 {
			prev_log_term = -1
		} else {
			//itm := r.VolatileStore[r.LastLogNr]
			itm := r.GetLog(r.LastLogNr)
			if itm == nil {
				prev_log_term = -1
			} else {
				prev_log_term = itm.Term
			}
		}
		smm := AppendEntries{Term: r.CurrentTerm, LeaderId: r.MyPid, PrevLogIndex: r.LastLogNr, PrevLogTerm: prev_log_term, LogData: &logit, LeaderCommit: r.CommitIndex}
		r.LastLogNr++
		//r.VolatileStore[r.LastLogNr] = logit
		r.PutLog(r.LastLogNr, &logit)

		/* Will loop until we found majority have responded with OK append entries or we are no longer leader
		The event when we get majority of the response we can safely save it and will take the next request in the outbox.
		*/
		totResp := 1
		tmparr := make([]int, r.TotalPeer)
		for vari := 0; vari < r.TotalPeer; vari++ { //i think this is not req
			tmparr[vari] = 0
		}
		/*one legitimate assumptions is that peers pid will be serial .. 1-MAX .. however, with some more state maintenance
		we can also go without this*/
		r.BackServer.Outbox() <- &Envelope{Pid: -1, MsgId: 2, Msg: smm}
		for {
			if totResp >= (r.TotalPeer/2 + r.TotalPeer%2) {
				break //can accept the MSG do the needful
			}
			if r.IsLeader() == false { //if not leader .. drop the process of outbox processing
				break
			}
			select {
			case strs := <-r.TempChanOB: //make this channel .. it will only have response to Apend Entry Req
				//strs, _ := ae_resp.Msg.(AEResponse) //now strs is LogItem
				rec_from := strs.SenderId //will have who have sent this response
				if strs.PrevLogIndex == r.CommitIndex && tmparr[rec_from] == 0 {
					tmparr[rec_from] = 1
					r.P("Got a response from :", rec_from, tmparr[rec_from])
					//also ensure LogIndex other then the expected will not send here
					totResp++
				}
			case <-time.After(time.Duration(r.TimeOutMin/3) * time.Millisecond):
				r.BackServer.Outbox() <- &Envelope{Pid: -1, MsgId: 2, Msg: smm}

			} //end of select
		}
		if r.IsLeader() == false {
			continue
		}
		r.CommitIndex = r.LastLogNr //available to me
		r.PutCom()
		//Tell all the participating server to lock their entry in their log
		r.SendAHBM <- 1
		r.InboxChan <- &logit //giving to my one inbox channel
		time.Sleep(60 * time.Millisecond)
		//Make the item available to the inbox
		stt, _ := logit.Data.(string) //just for testing purpose .. delete it
		r.P(string("Msg Replicated Successfully: "+stt), r.CommitIndex, totResp)
		r.OutboxInProg <- 1
	}
}

/*
func InboxRoutine(r *Replicator){
	for{
		select{
			case msgr:= <- r.InboxChan:
				//fmt.Println(string("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$["+strconv.Itoa(r.MyPid)+"]I received a msg: "+msgr.Data.(string)))
				//r.Publisher.SendMessage(string("["+strconv.Itoa(r.MyPid)+"]I received a msg: "+msgr.Data.(string)))
		}
	}
}*/
