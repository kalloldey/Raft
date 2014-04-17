package replicator

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"os"
	"strconv"
	"strings"
	"sync"
	//"encoding/xml"
)

/* Need To Support:
   Pid() int
   Peers() []int
   AddPeer(n int)
   DelPeer(n int)
   Outbox() chan *Envelope
   Inbox() chan *Envelope
*/
/* Way followed for connection: While sending msg act as client to the remote server
While want to receive packet be a server, remote client will send you packet. */

const MAX_PEER int = 1000
const ERR int = 404

//Actual server object
type Raftserver struct {
	MyPid      int
	TotalPeer  int
	PeersPid   [MAX_PEER]int
	Server     *zmq.Socket
	OwnEnd     string
	PeerHandle string
	StartAddr  int
	Fin        sync.WaitGroup
	RecChan    chan *Envelope
	OutChan    chan *Envelope
	//Need some handler or identification for connection purpose...
} //WR

//Object to read json file

var settings struct {
	SelfHandle  string `json:"selfHandle"`
	PeersPid    string `json:"peersPid"`
	PeersHandle string `json:"peersHandle"`
	StartAddr   int    `json:"startAddress"`
	StartMsgId  int    `json:"startMsgId"`
	BufferSize  int    `json:"bufSize"`
}

func (r Raftserver) Pid() int {
	return r.MyPid
} //FYN

func (r Raftserver) Wait() {
	r.Fin.Wait()
}
func (r Raftserver) Peers() []int {
	return r.PeersPid[0:]
} //FYN

func (r Raftserver) AddPeer(n int) {
	r.PeersPid[r.TotalPeer] = n
	r.TotalPeer = r.TotalPeer + 1
} //FYN

func (r Raftserver) DelPeer(n int) {
	if r.TotalPeer == 1 {
		r.PeersPid[0] = ERR
		r.TotalPeer = 0
	} else if r.TotalPeer == 0 {
		return
	} else {
		for i := 0; i < r.TotalPeer; i++ {
			if r.PeersPid[i] == n {
				r.PeersPid[i] = r.PeersPid[r.TotalPeer-1]
				r.TotalPeer = r.TotalPeer - 1
				break
			}
		}
	}
} //FYN

//This will directly accept the arguments
/*New_DirectArg(self pid, "pid of peers in a comma separated string", Strat address of the port as int, Self handle in string, Peers handle in string)*/
func New_DirectArg(PidArg int, ArgPeersPid string, ArgStartAddr int, ArgSelfHandle string, ArgPeersHandle string) *Raftserver { //To create the server object
	temp := strings.Split(ArgPeersPid, ",")
	rfs := new(Raftserver) //Instatiate a private server object
	j := 0
	for i := 0; i < len(temp); i++ {
		tm, _ := strconv.Atoi(temp[i])
		if tm != PidArg {
			rfs.PeersPid[j] = tm
			j++
		}
	}
	//File data read ended ....
	rfs.MyPid = PidArg
	rfs.TotalPeer = len(temp) - 1
	//	fmt.Println("[Instantiate]Total Peer: ",rfs.TotalPeer)
	//Read Peer nethandle form file
	rfs.StartAddr = ArgStartAddr
	//	fmt.Println("[New]:startaddr", rfs.StartAddr)
	rfs.PeerHandle = ArgPeersHandle
	rfs.OwnEnd = string(ArgSelfHandle + strconv.Itoa(rfs.StartAddr+rfs.MyPid)) //read from file
	//	fmt.Println("Own End point =",rfs.OwnEnd)
	//Do configure the netowork setup portion here
	rfs.Server, _ = zmq.NewSocket(zmq.PULL)
	rfs.Server.Bind(rfs.OwnEnd)
	//	rfs.Fin.Add(1)
	//Make the receive and send channels ..
	//	fmt.Println("Server Instantion done .. returning..")
	rfs.RecChan = make(chan *Envelope, 100)
	go rfs.recRoutine()
	rfs.OutChan = make(chan *Envelope, 100)
	go rfs.sendRoutine()
	return rfs
} //WR

//This will take filename as argument
func New(FileName string, PidArg int) *Raftserver { //To create the server object
	//File read starts ....
	configFile, err := os.Open(FileName)
	if err != nil {
		fmt.Println("opening config file: ", FileName, "..", err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&settings); err != nil {
		fmt.Println("Error in parsing config file ", err.Error())
	}
	temp := strings.Split(settings.PeersPid, ",")
	rfs := new(Raftserver) //Instatiate a private server object
	j := 0
	for i := 0; i < len(temp); i++ {
		tm, _ := strconv.Atoi(temp[i])
		if tm != PidArg {
			rfs.PeersPid[j] = tm
			j++
		}
	}
	//File data read ended ....
	rfs.MyPid = PidArg
	rfs.TotalPeer = len(temp) - 1
	//	fmt.Println("[Instantiate]Total Peer: ",rfs.TotalPeer)
	//Read Peer nethandle form file
	rfs.StartAddr = settings.StartAddr
	//	fmt.Println("[New]:startaddr", rfs.StartAddr)
	rfs.PeerHandle = settings.PeersHandle
	rfs.OwnEnd = string(settings.SelfHandle + strconv.Itoa(rfs.StartAddr+rfs.MyPid)) //read from file
	//	fmt.Println("Own End point =",rfs.OwnEnd)
	//Do configure the netowork setup portion here
	rfs.Server, _ = zmq.NewSocket(zmq.PULL)
	rfs.Server.Bind(rfs.OwnEnd)
	//Make the receive and send channels ..
	//	fmt.Println("Server Instantion done .. returning..")
	rfs.RecChan = make(chan *Envelope, 100)
	rfs.OutChan = make(chan *Envelope, 100)
	go rfs.recRoutine()
	go rfs.sendRoutine()
	return rfs
} //WR

func (r *Raftserver) sendRoutine() {
	//	fmt.Println("[send Routine] start")
	//Broadcast or Unicast
	for {
		om := <-r.OutChan
		//	fmt.Println("[send:] to pid: ", om.Pid)
		//		sendmsg, _ := json.Marshal(om) //Initial
		mCache := new(bytes.Buffer)
		encCache := gob.NewEncoder(mCache)
		gob.Register(AppendEntries{})
		gob.Register(AEResponse{})
		gob.Register(LogItem{})
		gob.Register(RequestVote{})
		gob.Register(SMCommand{})
		gob.Register(SMResp{})
		encCache.Encode(om)
		sendmsg := mCache.Bytes()
		//var interEnv Envelope
		//            json.Unmarshal(sendmsg, &interEnv)
		//fmt.Println("Before sending envelope", om)

		if om.Pid == -1 { //Do a Broadcast
			for i := 0; i < r.TotalPeer; i++ { //Improve it by calling simultaneous goroutines instead of this ordered flow
				endpoint := string(r.PeerHandle + strconv.Itoa(r.StartAddr+r.PeersPid[i]))
				client, _ := zmq.NewSocket(zmq.PUSH)
				client.Connect(endpoint)
				//			fmt.Println("[Server ]", r.MyPid,"sending msg to: ",r.PeersPid[i])
				client.SendBytes(sendmsg, 0)
				client.Close()
			}
		} else { //Do a unicast
			endpoint := string(r.PeerHandle + strconv.Itoa(r.StartAddr+om.Pid))
			client, _ := zmq.NewSocket(zmq.PUSH)
			//                fmt.Println("[send endpoint]:  ",endpoint)
			client.Connect(endpoint)
			client.SendBytes(sendmsg, 0)
			client.Close()
		}
	}
}

func (r Raftserver) Outbox() chan *Envelope {
	return r.OutChan
} //FYN

/*
func UnMarshalObj(Type int, inp *Envelope) {

	if Type == 0{
		return
	}else if Type == 2 {
		obj := inp.Msg.(map[string]interface{})
		var ams AppendEntries
		ams.Term = int(obj["Term"].(float64))
		ams.LeaderId = int(obj["LeaderId"].(float64))
		ams.PrevLogIndex = int(obj["PrevLogIndex"].(float64))
		ams.PrevLogTerm = int(obj["PrevLogTerm"].(float64))

		if obj["LogData"] == nil {
			ams.LogData = nil
			//fmt.Println("logData NULL")
		}else{
			//fmt.Println("LogData not null")
			var logd LogItem
			tmplog := obj["LogData"].(map[string]interface{})
			logd.Index = int(tmplog["Index"].(float64))
			logd.Term = int(tmplog["Term"].(float64))
			logd.Data = tmplog["Data"]
			ams.LogData = &logd
			//fmt.Println("Log Data : ",logd.Data)
		}
		ams.LeaderCommit = int(obj["LeaderCommit"].(float64))
		inp.Msg = ams
	}else if( Type == 3){
		obj := inp.Msg.(map[string]interface{})
                var ams AEResponse
                ams.Term = int(obj["Term"].(float64))
                ams.SenderId = int(obj["SenderId"].(float64))
                ams.PrevLogIndex = int(obj["PrevLogIndex"].(float64))
                ams.Type = int(obj["Type"].(float64))
		ams.Accept = bool(obj["Accept"].(bool))
                inp.Msg = ams
	}

} */

func (r *Raftserver) recRoutine() {
	for {
		//	fmt.Println("[Inbox]-entry befor RecvMsg")
		msg, err := r.Server.RecvBytes(0)
		if err != nil {
			//fmt.Println("Error in receiving msg ..")
		}
		//fmt.Println("[Inbox of ",r.MyPid,"] Msg received", msg)
		var interEnv Envelope
		pCache := bytes.NewBuffer(msg)
		decCache := gob.NewDecoder(pCache)
		decCache.Decode(&interEnv)

		//fmt.Println("Received envelope", interEnv)
		r.RecChan <- &interEnv
	}
}

func (r Raftserver) Inbox() chan *Envelope {
	return r.RecChan
} //FYN
