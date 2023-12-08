package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// KeyValue Map functions return a slice of KeyValue.
	KeyValue struct {
		Key   string
		Value string
	}
	ByKey []KeyValue
)

// for sorting by key.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	workerId := strconv.Itoa(os.Getpid())            // 创建WorkerId
	maxConcurrency := 10                             // 定义任务最大任务数
	maxConcurrencyChannel := make(chan struct{}, 10) // 定义任务最大任务数
	taskQueue := make(chan Task, 10)                 // 存储任务
	// 连接协调者
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()
	// 如果任务数小于 MaxConcurrency 定期向协调者询问任务，最大并行10个
	go func() {
		for {
			if len(taskQueue) < maxConcurrency {
				args := AskTaskArgs{WorkerId: workerId}
				reply := AskTaskReply{}
				log.Println("ask task...")
				if err = client.Call("Coordinator.AskTask", &args, &reply); err != nil {
					log.Println("ask task error:", err)
					if errors.Is(err, rpc.ErrShutdown) {
						close(taskQueue)
						break
					}
				}
				if reply.Task.Type != "" {
					log.Println("receive task ", reply.Task)
					taskQueue <- reply.Task
					reportIdle(client, workerId, reply.Task.Type, reply.Task.Id)
				}
			}
			time.Sleep(time.Second)
		}
	}()
	// 处理任务，控制最大任务数
	for task := range taskQueue {
		maxConcurrencyChannel <- struct{}{}
		go func(task Task) {
			defer func() {
				<-maxConcurrencyChannel
			}()
			reportProgress(client, workerId, task.Type, task.Id) // 报告任务进行中
			var err error
			defer func() {
				if err != nil {
					reportError(client, workerId, task.Type, task.Id, err) // 报告任务出现错误
				} else {
					reportCompleted(client, workerId, task.Type, task.Id) // 报告任务已完成
				}
			}()
			switch task.Type {
			case MAP:
				// 读取分区
				bytes, err := os.ReadFile(task.Split)
				if err != nil {
					log.Println("don't read split:", task.Split, err)
					return
				} else {
					content := string(bytes)
					keyValues := mapf(task.Split, content)
					marshal, err := json.Marshal(keyValues)
					if err != nil {
						log.Println("don't json marshal keyValues:", task.Split, err)
						return
					}
					if err = os.WriteFile(task.IntermediateFile, marshal, os.ModePerm); err != nil {
						log.Println("don't write intermediate file:", task.Split, task.IntermediateFile, err)
						return
					}
				}
			case REDUCE:
				// 获取完成的中间文件
				receiveCount := 0
				finalSum := 0
				intermediateFileChannel := make(chan string, 10)
				go func() {
					//defer func() {
					//	if err != nil {
					//		reportError(client, workerId, task.Type, task.Id, err) // 报告任务出现错误
					//	} else {
					//		reportCompleted(client, workerId, task.Type, task.Id) // 报告任务已完成
					//	}
					//}()
					for {
						args := GetIntermediateFilesArgs{
							WorkerId:     workerId,
							ReduceTaskId: task.Id,
							ReceiveCount: receiveCount,
						}
						reply := GetIntermediateFilesReply{}
						if err = client.Call("Coordinator.GetIntermediateFiles", &args, &reply); err != nil {
							log.Println("client call Coordinator.GetIntermediateFiles failed:", err)
							return
						}
						for _, file := range reply.IntermediateFiles {
							intermediateFileChannel <- file
						}
						receiveCount += len(reply.IntermediateFiles)
						if receiveCount >= reply.Sum {
							finalSum = reply.Sum
							close(intermediateFileChannel)
							log.Println("all intermediate file has received", finalSum)
							break
						}
						time.Sleep(time.Second)
					}
				}()
				// 获取NReduce（为了计算相同key的桶位）
				args2 := GetNTaskArgs{
					WorkerId: workerId,
				}
				reply2 := GetNTaskReply{}
				if err = client.Call("Coordinator.GetNTask", &args2, &reply2); err != nil {
					log.Println("client call Coordinator.GetNReduce failed:", err)
					return
				}
				nReduce := reply2.NReduce
				nMap := reply2.NMap
				// 处理中间文件
				maxReduceConcurrency := 5
				maxReduceConcurrencyChannel := make(chan struct{}, maxReduceConcurrency)

				mmapMutex := sync.Mutex{}
				mmap := make(map[string][]string)
				//smap := sync.Map{}
				tampOfile, err := os.Create(task.OutputFile + ".tamp")
				if err != nil {
					log.Println("os create tamp output file failed:", err)
					return
				}
				handleIntermediateNumber := atomic.Int64{}
				for intermediateFile := range intermediateFileChannel {
					maxReduceConcurrencyChannel <- struct{}{}
					go func(intermediateFile string) {
						defer func() {
							<-maxReduceConcurrencyChannel
						}()
						//defer func() {
						//	if err != nil {
						//		reportError(client, workerId, task.Type, task.Id, err) // 报告任务出现错误
						//	} else {
						//		reportCompleted(client, workerId, task.Type, task.Id) // 报告任务已完成
						//	}
						//}()
						defer handleIntermediateNumber.Add(1)
						// 读取中间文件
						bytes, err := os.ReadFile(intermediateFile)
						if err != nil {
							log.Println("read intermediate file failed:", err)
							return
						}
						var keyValues []KeyValue
						if err = json.Unmarshal(bytes, &keyValues); err != nil {
							log.Println("json unmarshal intermediate file content failed:", err)
							return
						}
						sort.Sort(ByKey(keyValues))
						left := 0
						right := 0
						for i := 0; i < len(keyValues); i++ {
							if i+1 == len(keyValues) || keyValues[i+1].Key != keyValues[left].Key {
								if h := (ihash(keyValues[left].Key) % nReduce) + nMap; h != task.Id {
									left = i + 1
									right = i + 1
									continue
								}
								right = i
								values := make([]string, 0, right-left+1)
								for j := left; j <= right; j++ {
									values = append(values, keyValues[j].Value)
								}
								mmapMutex.Lock()
								if vs, ok := mmap[keyValues[left].Key]; !ok {
									mmap[keyValues[left].Key] = values
								} else {
									vs = append(vs, values...)
									mmap[keyValues[left].Key] = append(mmap[keyValues[left].Key], values...)
								}
								mmapMutex.Unlock()
								left = i + 1
								right = i + 1
							}
						}
					}(intermediateFile)
				}
				for handleIntermediateNumber.Load() != int64(finalSum) {
					time.Sleep(time.Second)
				}
				// 将output内容写入output文件文件中
				results := make([]KeyValue, 0)
				for k, vs := range mmap {
					results = append(results, KeyValue{k, reducef(k, vs)})
				}
				sort.Sort(ByKey(results))
				for _, r := range results {
					if _, err = fmt.Fprintf(tampOfile, "%v %v\n", r.Key, r.Value); err != nil {
						log.Println("fmt fprintf tamp output file failed:", err)
						return
					}
				}
				// 将文件重命名
				if err = os.Rename(task.OutputFile+".tamp", task.OutputFile); err != nil {
					log.Println("rename tamp output file failed:", err)
					return
				}
			}
		}(task)
	}
}

func reportCompleted(client *rpc.Client, workerId string, taskType string, taskId int) {
	args := ReportTaskStateArgs{
		WorkerId: workerId,
		Type:     taskType,
		Id:       taskId,
		State:    COMPLETED,
	}
	reply := ReportTaskStateReply{}
	if err := client.Call("Coordinator.ReportTaskState", &args, &reply); err != nil {
		log.Println("report task state error:", err)
	}
}

func reportProgress(client *rpc.Client, workerId string, taskType string, taskId int) {
	args := ReportTaskStateArgs{
		WorkerId: workerId,
		Type:     taskType,
		Id:       taskId,
		State:    PROGRESS,
	}
	reply := ReportTaskStateReply{}
	if err := client.Call("Coordinator.ReportTaskState", &args, &reply); err != nil {
		log.Println("report task state error:", err)
	}
}

func reportIdle(client *rpc.Client, workerId string, taskType string, taskId int) {
	args := ReportTaskStateArgs{
		WorkerId: workerId,
		Type:     taskType,
		Id:       taskId,
		State:    IDLE,
	}
	reply := ReportTaskStateReply{}
	if err := client.Call("Coordinator.ReportTaskState", &args, &reply); err != nil {
		log.Println("report task state error:", err)
	}
}

func reportError(client *rpc.Client, workerId string, taskType string, taskId int, err error) {
	args := ReportTaskStateArgs{
		WorkerId: workerId,
		Type:     taskType,
		Id:       taskId,
		Err:      err,
		State:    ERROR,
	}
	reply := ReportTaskStateReply{}
	if err = client.Call("Coordinator.ReportTaskState", &args, &reply); err != nil {
		log.Println("report task error error:", err)
	}
}
