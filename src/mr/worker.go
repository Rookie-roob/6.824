package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		task_args := TaskArgs{}
		task_reply := TaskReply{}
		ok := call("Master.AllocateTask", &task_args, &task_reply)
		if !ok || task_reply.TaskType == 3 {
			break
		} else if task_reply.TaskType == 2 {
			time.Sleep(time.Second)
		} else if task_reply.TaskType == 0 {
			intermediate := []KeyValue{}
			map_id := task_reply.MapTaskId
			file, err := os.Open(task_reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", task_reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task_reply.FileName)
			}
			file.Close()
			kva := mapf(task_reply.FileName, string(content))
			intermediate = append(intermediate, kva...)
			map_product := make([][]KeyValue, task_reply.TotalReduce)
			for i := 0; i < task_reply.TotalReduce; i++ {
				map_product[i] = []KeyValue{}
			}
			for _, item := range intermediate {
				map_product[ihash(item.Key)%task_reply.TotalReduce] = append(map_product[ihash(item.Key)%task_reply.TotalReduce], item)
			}
			for i := range map_product {
				output_file_name := "mr-" + strconv.Itoa(map_id) + "-" + strconv.Itoa(i)
				tmp_file, _ := ioutil.TempFile("", output_file_name)
				enc := json.NewEncoder(tmp_file)
				for _, item := range map_product[i] {
					err := enc.Encode(&item)
					if err != nil {
						log.Fatalf("write file %v failed", output_file_name)
					}
				}
				os.Rename(tmp_file.Name(), output_file_name)
				tmp_file.Close()
			}
			finish_map_args := FinishMapArgs{}
			finish_map_args.MapTaskId = map_id
			finish_map_reply := FinishMapReply{}
			call("Master.FinishMap", &finish_map_args, &finish_map_reply) // master is supposed not to be dead accidentally
		} else {
			reduce_id := task_reply.ReduceTaskId
			source_filename := ""
			total_res := []KeyValue{}
			for i := range task_reply.TotalMap {
				source_filename = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduce_id)
				file, err := os.Open(source_filename)
				if err != nil {
					log.Fatalf("read file %v failed", source_filename)
				}
				dec := json.NewDecoder(file)
				kva := []KeyValue{}
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				total_res = append(total_res, kva...)
			}
			sort.Sort(ByKey(total_res))
			oname := "mr-out-" + strconv.Itoa(reduce_id)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(total_res) {
				j := i + 1
				for j < len(total_res) && total_res[j].Key == total_res[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, total_res[k].Value)
				}
				output := reducef(total_res[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", total_res[i].Key, output)

				i = j
			}
			ofile.Close()
			for i := range task_reply.TotalMap {
				source_filename = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduce_id)
				err := os.Remove(source_filename)
				if err != nil {
					log.Fatalf("remove file %v failed", source_filename)
				}
			}
			finish_reduce_args := FinishReduceArgs{}
			finish_reduce_args.ReduceTaskId = reduce_id
			finish_reduce_reply := FinishReduceReply{}
			call("Master.FinishReduce", &finish_reduce_args, &finish_reduce_reply)
		}
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
