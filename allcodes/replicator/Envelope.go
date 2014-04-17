/*************************************************************
Author : Kallol Dey
Description: Package with Envelope format and Server interface.
This rules need to be followed by every member of the cluster
***************************************************************/

package replicator

const (
	BROADCAST = -1
)

type Envelope struct {
	Pid   int // Pid is always set to the receiver ... it will be -1 when broadcast
	MsgId int //Unique id number for message
	Msg   interface{}
}

type Server interface {
	Pid() int
	Peers() []int
	AddPeer(n int)
	DelPeer(n int)
	Outbox() chan *Envelope
	Inbox() chan *Envelope
}
