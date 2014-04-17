package kv

import (
//	"fmt"
	"github.com/jmhodges/levigo"
	"math/rand"
	"os/exec"
	"strconv"
	"testing" //import go package for testing related functionality
	"time"
)

const strpath string = "allcodes/"
const TOTSER int = 5
const DATADB string = "/tmp/KVSDB_"
const TOTALCOMMAND int =250   //100,1000

func Test_General(t *testing.T) {

	totcmd := TOTALCOMMAND
	var putst [1000]int
	//Preprocessing :
	for i := 0; i < TOTSER; i++ {
		x := strconv.Itoa(i)
		exec.Command("rm", "-rf", string("/tmp/KVSDB_"+x)).Start()
		exec.Command("rm", "-rf", string("/tmp/LOGDB_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/TERMDB_"+x)).Start()
	}

	var cmd [TOTSER]*exec.Cmd
	for i := 0; i < TOTSER; i++ {
		cmd[i] = exec.Command(string(strpath+"ReplicatorMain"), string(strpath+"replicator/config.json"), strconv.Itoa(i), string(strpath+"replicator/kvconfig.json"))
		err := cmd[i].Start()
		if err != nil {
			t.Error("Problem in executing the process of KVstore")
		}
	}
	//fmt.Println("Giving a intro sleep to allow a leader to be elected and settle down")
	time.Sleep(time.Duration(12000) * time.Millisecond)

	kvc := GetKVC("allcodes/replicator/kvconfig.json", 5) //5 because I have 0-4 as KVserver(total 5) and 5th one is KVClient
	var kill int
	for j := 0; j < totcmd; j++ {
		putst[j] = 0
		key := string("NotAnyKey" + strconv.Itoa(j))
		value := string(key + "__VALUE")
		kvc.SetReqId(j)
		ret := kvc.Put([]byte(key), []byte(value))
		if ret == 0 {
			putst[j] = 1
		}

		if (j+5)%22 == 0 {
			kill = rand.Intn(TOTSER)
			cmd[kill].Process.Kill()
			//	fmt.Println("Process Killed: ",kill)
		}
		if (j+1)%22 == 0 {
			cmd[kill] = exec.Command(string(strpath+"ReplicatorMain"), string(strpath+"replicator/config.json"), strconv.Itoa(kill), string(strpath+"replicator/kvconfig.json"))
			err := cmd[kill].Start()
			if err != nil {
				t.Error("Rep_test says :Some Error in restarting a KV server process")
			}
			//fmt.Println("Process Resumed: ",kill)

		}

	}

	for j := 0; j < totcmd; j++ {
		key := string("NotAnyKey" + strconv.Itoa(j))
		kvc.SetReqId(90)
		_, err := kvc.Get([]byte(key))
		if err == 1 {
			if putst[j] == 1 {
				t.Error("Successful get not found in GET")
			}
		}
	}

	//fmt.Println("Give a precautionary sleep before stopping all servers for consistency check")
	time.Sleep(time.Duration(50000) * time.Millisecond)

	//Stop All Servers Execution
	for i := 0; i < TOTSER; i++ {
		err := cmd[i].Process.Kill()
		if err != nil {
			//            log.Fatal("failed to kill: ", err)
			//fmt.Println("Failed to kill")
		}
	}
	time.Sleep(time.Duration(10000) * time.Millisecond) // req so that DB lock left

	//Analysis Portion:
	//Collect DB name and commit index of log for all replicator
	var DDb [TOTSER]*levigo.DB
	for i := 0; i < TOTSER; i++ {
		dat := string(DATADB + strconv.Itoa(i))
		opts := levigo.NewOptions()
		opts.SetCache(levigo.NewLRUCache(3 << 30))
		//opts.SetCreateIfMissing(true)
		var err error
		DDb[i], err = levigo.Open(dat, opts)
		if err != nil {
			t.Error("Error in obtaining DB instance:", err.Error())
		}
	}

	//Check all the kv store's data  db
	for j := 0; j < totcmd; j++ {
		key := string("NotAnyKey" + strconv.Itoa(j))
		var x string
		for i := 0; i < TOTSER; i++ {
			ro := levigo.NewReadOptions()
			val, err := DDb[i].Get(ro, []byte(key))
			if err != nil {
				t.Error("Data not found in db")
			} else {
				if i == 0 {
					x = string(val)
				}
				if x != string(val) {
					t.Error(string(strconv.Itoa(j) + " Data for a key not matched accros DATA DB" + strconv.Itoa(i)))
				}
			}
		}
	}

}
