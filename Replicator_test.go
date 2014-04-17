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

const TESTLEVEL int=22
const strpath string = "allcodes/"
const TOTSER int = 5
const DATADB string = "/tmp/KVSDB_"
const TOTALCOMMAND int = 250 //100,1000
const CONCCOMMAND int = 75
const TOTSER_STRESS int = 9  //In the stress test total number of kv server
const MAXKILL_STRESS int = 4 //In the stress test how many servers can be died at most at any time
const CONCCOMMAND_STRESS int = 12 //In the stress test how many command will be issued by each parallel client
const TESTLEVEL_STRESS int=35   //In the stress test at what iteration cycle will kill the KVs


/*
What this test will do?
Start all the KV servers and wait for some time to settle them down
Will issue a sequence of put request
Kill KV servers randomly on iteration number based on TESTLEVEL and resume them, 5 iteration after they are killed
Get request issued to check if it found all the successful puts
Stopped all the servers 
Check the level DB for data of all the servers and check their consistency
This will have only single end client issuing request, multi client is done on other test
*/

func Test_General(t *testing.T) {

	totcmd := TOTALCOMMAND
	var putst [1000]int
	//Preprocessing :
	for i := 0; i < TOTSER; i++ {
		x := strconv.Itoa(i)
		exec.Command("rm", "-rf", string("/tmp/KVSDB_"+x)).Start()
		exec.Command("rm", "-rf", string("/tmp/LOGDB_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/TERM_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/KVDEBUG_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/DEBUG_REPL_"+x)).Start()
	}

	var cmd [TOTSER]*exec.Cmd
	for i := 0; i < TOTSER; i++ {
		cmd[i] = exec.Command(string(strpath+"ReplicatorMain"), "configs/config.json", strconv.Itoa(i), "configs/kvconfig.json")
		err := cmd[i].Start()
		if err != nil {
			t.Error("Problem in executing the process of KVstore")
		}
	}
//	fmt.Println("Giving a intro sleep to allow a leader to be elected and settle down")
	time.Sleep(time.Duration(12000) * time.Millisecond)

	kvc := GetKVC("configs/kvconfig.json", 5) //5 because I have 0-4 as KVserver(total 5) and 5th one is KVClient
	var kill int
	for j := 0; j < totcmd; j++ {
		putst[j] = 0
		key := string("NotAnyKey" + strconv.Itoa(j))
		value := string(key + "__VALUE")
		kvc.SetReqId(j)
		ret := kvc.Put([]byte(key), []byte(value))
		if ret == 0 {
//			fmt.Println("Put success")
			putst[j] = 1
		}

		if (j+5)%TESTLEVEL == 0 {
			kill = rand.Intn(TOTSER)
			cmd[kill].Process.Kill()
			//	fmt.Println("Process Killed: ",kill)
		}
		if (j+1)%TESTLEVEL == 0 {
			cmd[kill] = exec.Command(string(strpath+"ReplicatorMain"), "configs/config.json", strconv.Itoa(kill), "configs/kvconfig.json")
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
				t.Error("Successful PUT not found in GET")
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
		var a *string
		a= &key
		for i := 0; i < TOTSER; i++ {
			ro := levigo.NewReadOptions()
			val, err := DDb[i].Get(ro, []byte(key))
			if err != nil {
				if i == 0{
					a = nil
				}
				if a != nil{
					t.Error("Data not found in db")
				}
			} else {
				if i == 0 {
					x = string(val)
				}
				if x != string(val) {
//					fmt.Println("Not match")
					t.Error(string(strconv.Itoa(j) + " Data for a key not matched accros DATA DB" + strconv.Itoa(i)))
				}
			}
		}
	}

}


/*
What this test will do?
Introducing testing for multiple client
Multiple client will parallely issue put requests
In the end all servers are stopped
Checked consistency of the DB of all the KVserver
*/

func Test_MultiClient(t *testing.T) {

	totcmd := CONCCOMMAND
	//Preprocessing :
	for i := 0; i < TOTSER; i++ {
		x := strconv.Itoa(i)
		exec.Command("rm", "-rf", string("/tmp/KVSDB_"+x)).Start()
		exec.Command("rm", "-rf", string("/tmp/LOGDB_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/TERM_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/KVDEBUG_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/DEBUG_REPL_"+x)).Start()
	}

	var cmd [TOTSER]*exec.Cmd
	for i := 0; i < TOTSER; i++ {
		cmd[i] = exec.Command(string(strpath+"ReplicatorMain"), "configs/multconfig.json", strconv.Itoa(i), "configs/multkvconfig.json")
		err := cmd[i].Start()
		if err != nil {
			t.Error("Problem in executing the process of KVstore")
		//	fmt.Println("Probelm in creating process")
		}
	}
//	fmt.Println("Giving a intro sleep to allow a leader to be elected and settle down")
	done := make(chan *int, totcmd*3)
	time.Sleep(time.Duration(12000) * time.Millisecond)
	go Run_KVC("configs/multkvconfig.json", 5, done, totcmd)
	go Run_KVC("configs/multkvconfig.json", 6, done, totcmd)
	go Run_KVC("configs/multkvconfig.json", 7, done, totcmd)
//	fmt.Println("Done creating kvs")
	var kill int
	for j := 0; j < totcmd*3; j++ {
		<-done
		//fmt.Println("Put Done")
		if (j+5)%TESTLEVEL == 0 {
			kill = rand.Intn(TOTSER)
			cmd[kill].Process.Kill()
				//fmt.Println("Process Killed: ",kill)
		}
		if (j+1)%TESTLEVEL == 0 {
			cmd[kill] = exec.Command(string(strpath+"ReplicatorMain"), "configs/multconfig.json" , strconv.Itoa(kill), "configs/multkvconfig.json")
			err := cmd[kill].Start()
			if err != nil {
				t.Error("Rep_test says :Some Error in restarting a KV server process")
			}
			//fmt.Println("Process Resumed: ",kill)
		}
	}

//	fmt.Println("Give a precautionary sleep before stopping all servers for consistency check")
	time.Sleep(time.Duration(20000) * time.Millisecond)

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
	for p := 5; p < 8; p++ {
		for j := 0; j < totcmd; j++ {
			key := string("AnyKey" + strconv.Itoa(j) + "_" + strconv.Itoa(p))
			var x string
			var a *string
			a= &key
			for i := 0; i < TOTSER; i++ {
				ro := levigo.NewReadOptions()
				val, err := DDb[i].Get(ro, []byte(key))
				if err != nil {
					if i == 0{
						a = nil
					}
					if a != nil{
						t.Error("Data not found in db")
					}
				} else {
					if i == 0 {
						x = string(val)
					}
					if x != string(val) {
						t.Error(string(strconv.Itoa(j) + " Data for a key not matched accros DATA DB" + strconv.Itoa(i)))
						//fmt.Println(string(strconv.Itoa(j) + " Data for a key not matched accros DATA DB" + strconv.Itoa(i)))
					}
				}
			}
		}
	}

}


/*
What this test will do?
Introducing stress testing 
Large number of KV servers = 9
Large number of End Client = 5
Will Kill MULTIPLE kv servers at a time 
Checked consistency of the DB of all the KVserver
*/
/*
func Test_Stress(t *testing.T) {

	totcmd := CONCCOMMAND_STRESS
	//Preprocessing :
	for i := 0; i < TOTSER_STRESS; i++ {
		x := strconv.Itoa(i)
		exec.Command("rm", "-rf", string("/tmp/KVSDB_"+x)).Start()
		exec.Command("rm", "-rf", string("/tmp/LOGDB_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/TERM_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/KVDEBUG_"+x)).Start()
		exec.Command("rm", "-f", string("/tmp/DEBUG_REPL_"+x)).Start()
	}

	var cmd [TOTSER_STRESS]*exec.Cmd
	var st [TOTSER_STRESS]int
	for i := 0; i < TOTSER_STRESS; i++ {
		cmd[i] = exec.Command(string(strpath+"ReplicatorMain"), "configs/stressconfig.json", strconv.Itoa(i), "configs/stresskvconfig.json")
		err := cmd[i].Start()
		if err != nil {
			t.Error("Problem in executing the process of KVstore")
			//fmt.Println("Probelm in creating process")
		}
		st[i]=0
	}
	//fmt.Println("[Stress]Giving a intro sleep to allow a leader to be elected and settle down")
	done := make(chan *int, totcmd*5)
	time.Sleep(time.Duration(20000) * time.Millisecond)
	go Run_KVC("configs/stresskvconfig.json", 9, done, totcmd)
	go Run_KVC("configs/stresskvconfig.json", 10, done, totcmd)
	go Run_KVC("configs/stresskvconfig.json", 11, done, totcmd)
	go Run_KVC("configs/stresskvconfig.json", 12, done, totcmd)	
	go Run_KVC("configs/stresskvconfig.json", 13, done, totcmd)	
	//fmt.Println("Done creating kvs")
	var kill int

	for j := 0; j < totcmd*5; j++ {
		<-done
//		fmt.Println("Put Done")
		if (j+5)%TESTLEVEL_STRESS == 0 {
			totkill:= rand.Intn(MAXKILL_STRESS) //How many KV servers we will kill?
			if totkill <= 1{
				totkill = 2
			}
			//fmt.Println("Will Kill",totkill)
			for k:=0 ; k< totkill ;k++{ //lets kill
				kill = rand.Intn(TOTSER_STRESS)
				if st[kill] == 0{
					st[kill]=1
					cmd[kill].Process.Kill()
					//fmt.Println("Process Killed: ",kill)				
				}
			}

		}
		if (j+1)%TESTLEVEL_STRESS == 0 {
			for k:=0;k<TOTSER_STRESS;k++{
				if st[k] == 1{
					cmd[k] = exec.Command(string(strpath+"ReplicatorMain"), "configs/multconfig.json" , strconv.Itoa(k), "configs/multkvconfig.json")
					err := cmd[k].Start()
					if err != nil {
						t.Error("Rep_test says :Some Error in restarting a KV server process")
					}
					//fmt.Println("Process Resumed: ",k)
					st[k]=0
				}
			}
		}
	}

	//fmt.Println("Give a precautionary sleep before stopping all servers for consistency check")
	time.Sleep(time.Duration(30000) * time.Millisecond)

	//Stop All Servers Execution
	for i := 0; i < TOTSER_STRESS; i++ {
		err := cmd[i].Process.Kill()
		if err != nil {
			//            log.Fatal("failed to kill: ", err)
			//fmt.Println("Failed to kill")
		}
	}
	time.Sleep(time.Duration(20000) * time.Millisecond) // req so that DB lock left

	//Analysis Portion:
	//Collect DB name and commit index of log for all replicator
	var DDb [TOTSER_STRESS]*levigo.DB
	for i := 0; i < TOTSER_STRESS; i++ {
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
	for p := 9; p < 14; p++ {
		for j := 0; j < totcmd; j++ {
			key := string("AnyKey" + strconv.Itoa(j) + "_" + strconv.Itoa(p))
			var x string
			var a *string
			a = &key
			for i := 0; i < TOTSER_STRESS; i++ {
				ro := levigo.NewReadOptions()
				val, err := DDb[i].Get(ro, []byte(key))
				if err != nil {
					if i == 0{
						a = nil
					}
					if a != nil{
						t.Error("Data not found in db")
					}
				} else {
					if i == 0 {
						x = string(val)
					}
					if x != string(val) {
						t.Error(string(strconv.Itoa(j) + " Data for a key not matched accros DATA DB" + strconv.Itoa(i)))
					//	fmt.Println(string(strconv.Itoa(j) + " Data for a key not matched accros DATA DB" + strconv.Itoa(i)))
					}
				}
			}
		}
	}

}

*/
func Run_KVC(config string, pid int, done chan *int, totcmd int) {
	kvc := GetKVC(config, pid)
	for j := 0; j < totcmd; j++ {
		key := string("AnyKey" + strconv.Itoa(j) + "_" + strconv.Itoa(pid))
		value := string(key + "__VALUE")
		kvc.SetReqId(j)
		kvc.Put([]byte(key), []byte(value))
		va := 1
		done <- &va
	}

}
