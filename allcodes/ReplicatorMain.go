package main

import (
	"fmt"
	"os"
	"strconv"
	rep "github.com/kalloldey/raft/allcodes/replicator"
)

func main() {
	conf_replicator := os.Args[1]
	serverno, _ := strconv.Atoi(string(os.Args[2]))
	conf_outcomm := os.Args[3]
	//	fmt.Println("Conf: ",conf)
	//	fmt.Println("Server no: ",serverno)
	//      rep.GetNew(conf,serverno)  //just executing...
	rep.KV_init(conf_replicator, serverno, conf_outcomm)
	fmt.Println("Not End")
	cc := make(chan int, 1)
	<-cc
	fmt.Println("End")
}
