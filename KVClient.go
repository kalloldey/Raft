package kv

import (
	"encoding/json"
	"fmt"
	comm "github.com/kalloldey/raft/allcodes/replicator"
	"os"
	"time"
)

type KVClient interface {
	Put(key []byte, value []byte) int
	Get(key []byte) ([]byte, int)
}

type KVC struct {
	Server      *comm.Raftserver
	MyPid       int
	TotalServer int
	SlaTimeOut  int
	PingTo      int //to which server I should send request now
	ReqId       int //set this ReqId by calling setter and getter methods if you need to make request idempotent
}

func (kvc *KVC) Put(key []byte, value []byte) int {

	cmd := new(comm.SMCommand)
	cmd.Command = "PUT"
	cmd.Key = key
	cmd.Value = value
	cmd.UserId = kvc.MyPid
	cmd.RequestId = kvc.ReqId // or koi bhi random number
	kvc.PingTo = kvc.PingTo + 1
	sendto := kvc.PingTo % kvc.TotalServer
	kvc.Server.Outbox() <- &comm.Envelope{Pid: sendto, MsgId: 4, Msg: cmd}
	for {
		var res comm.SMResp
		select {
		case <-time.After(time.Duration(kvc.SlaTimeOut) * time.Millisecond):
			{
				fmt.Println("[put]Time Up")
				return 1
			}
		case val := <-kvc.Server.Inbox():
			{
				switch val.Msg.(type) {
				case comm.SMResp:
					res = val.Msg.(comm.SMResp)
					if res.Error == 1 {
						fmt.Println(cmd.RequestId,"Put Bad Response: ", string(res.Value))
						return 1
					} else {
						fmt.Println(cmd.RequestId,"Put Good Resp")
						return 0
					}
				}
			}
		}
	}
}

func (kvc *KVC) SetReqId(i int) {
	kvc.ReqId = i
}

func (kvc *KVC) GetReqId() int {
	return kvc.ReqId
}
func (kvc *KVC) Get(key []byte) ([]byte, int) {
	cmd := new(comm.SMCommand)
	cmd.Command = "GET"
	cmd.Key = key
	cmd.Value = nil
	cmd.UserId = kvc.MyPid
	cmd.RequestId = kvc.ReqId //koi bhi random number
	kvc.PingTo = kvc.PingTo + 1
	kvc.Server.Outbox() <- &comm.Envelope{Pid: kvc.PingTo % kvc.TotalServer, MsgId: 4, Msg: cmd}
	for {
		select {
		case <-time.After(time.Duration(kvc.SlaTimeOut) * time.Millisecond):
			{
				//fmt.Println("Get time out")
				return nil, 1
			}
		case val := <-kvc.Server.Inbox():
			{
				switch val.Msg.(type) {
                                case comm.SMResp:
				res := val.Msg.(comm.SMResp)
				if res.Error == 1 {
					//fmt.Println("Get not found")
					if res.Value != nil {
						//fmt.Println("Response value: ", string(res.Value))
					}
					return nil, 1
				} else {
					//fmt.Println("Get Found")
					return res.Value, 0
				}
				}
			}
		}
	}
}

//Initializing method for KV Client  will make a receive channel and will also provide put get interface
func GetKVC(ConfigFile string, MyPid int) *KVC {
	kvc := new(KVC)
	configFile, err := os.Open(ConfigFile)
	if err != nil {
		fmt.Println("opening config file: ", ConfigFile, "..", err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&comm.Conf); err != nil {
		fmt.Println("Error in parsing config file ", err.Error())
	}
	kvc.ReqId = comm.Conf.StartMsgId
	kvc.PingTo = 0
	kvc.TotalServer = comm.Conf.TotalServer
	kvc.MyPid = MyPid
	if kvc.MyPid < kvc.TotalServer { //error
		return nil
	}
	//func New_DirectArg(PidArg int, ArgPeersPid string, ArgStartAddr int, ArgSelfHandle string, ArgPeersHandle string)
	kvc.Server = comm.New_DirectArg(kvc.MyPid, comm.Conf.PeersPid, comm.Conf.StartAddr, comm.Conf.SelfHandle, comm.Conf.PeersHandle)
	kvc.SlaTimeOut = comm.Conf.SlaTimeOut
	return kvc
}
