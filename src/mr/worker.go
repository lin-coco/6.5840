package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	network = "tcp"
	address = "127.0.0.1:1234"
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

// 连接协调者
func connectCoordinator() *rpc.Client {
	client, err := rpc.DialHTTP(network, address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

// 生成workerId
func generateWorkerId() string {
	return strconv.Itoa(os.Getpid())
}

// 安全的协程
func safeGoroutine(fn func(), errorHandler func(interface{})) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recovered from panic: %v\n", r)
				// 处理panic
				if errorHandler != nil {
					errorHandler(r)
				} else {
					// 如果你不想让整个程序终止，可以选择不调用 runtime.Goexit()
					runtime.Goexit()
				}
			}
		}()
		fn()
	}()
}

var (
	client   *rpc.Client
	workerId string
	maxTask  int // 工作者同时处理最多的任务数量

)

func Init() {
	client = connectCoordinator()
	workerId = generateWorkerId()
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var (
		taskQueue = make(chan Task, maxTask) // 存储任务
	)
	go func() {
		// 访问当前的 goroutine 信息
		for {
			time.Sleep(5 * time.Second)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("Num Goroutine: %d\n", runtime.NumGoroutine())
			fmt.Printf("Num Block: %d\n", m.NumGC)
		}
	}()
	Init()
	// 询问任务 接收任务
	safeGoroutine(func() {
		for {
			time.Sleep(time.Second)
			log.Printf("ask task ...\n")
			task, err := askTask()
			if err != nil {
				log.Printf("error: ask task %v\n", err)
				if errors.Is(err, rpc.ErrShutdown) {
					close(taskQueue)
					break
				}
			}
			if task.Type == "" {
				continue
			}
			taskQueue <- task
			log.Printf("task idle... %v\n", task)
			reportIdle(client, workerId, task.Type, task.Id) // 报告任务等待中
		}
	}, nil)
	// 处理任务
	for task := range taskQueue {
		log.Printf("task progress...%v\n", task)
		reportProgress(client, workerId, task.Type, task.Id) // 报告任务进行中
		task := task
		switch task.Type {
		case MAP:
			safeGoroutine(func() {
				var err error
				defer func() {
					if err != nil {
						reportError(client, workerId, task.Type, task.Id, err) // 报告任务出现错误
						log.Printf("task error:%v...%v\n", err, task)
						return
					}
					reportCompleted(client, workerId, task.Type, task.Id)
					log.Printf("task completed...%v\n", task)
				}()
				err = handleMapTask(task, mapf)
			}, func(panic interface{}) {
				defer func() {
					reportError(client, workerId, task.Type, task.Id, panic) // 报告任务出现错误
					log.Printf("task panic:%v...%v\n", panic, task)
				}()
			})
		case REDUCE:
			safeGoroutine(func() {
				var err error
				defer func() {
					if err != nil {
						reportError(client, workerId, task.Type, task.Id, err) // 报告任务出现错误
						log.Printf("task error:%v...%v\n", err, task)
						return
					}
					reportCompleted(client, workerId, task.Type, task.Id)
					log.Printf("task completed...%v\n", task)
				}()
				err = handleReduceTask(task, reducef)
			}, func(panic interface{}) {
				defer func() {
					reportError(client, workerId, task.Type, task.Id, panic) // 报告任务出现错误
					log.Printf("task panic:%v...%v\n", panic, task)
				}()
			})
		}
	}
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) error {
	// 读取分区内容
	bytes, err := os.ReadFile(task.Split)
	if err != nil {
		return err
	}
	content := string(bytes)
	keyValues := mapf(task.Split, content)
	marshal, err := json.Marshal(keyValues)
	if err != nil {
		return err
	}
	// 写入中间文件
	if err = os.WriteFile(task.IntermediateFile, marshal, os.ModePerm); err != nil {
		return err
	}
	return nil
}
func handleReduceTask(task Task, reducef func(string, []string) string) error {
	var (
		err                         error
		receiveIntermediateCount    atomic.Int64
		finalIntermediateCount      atomic.Int64
		intermediateFiles           = make(chan string)
		maxHandleIntermediateCount  = make(chan struct{}, 5) // 最多同时处理的中间文件数量
		hasHandleIntermediateNumber = atomic.Int64{}         // 已经完成的中间文件数量
		mmapMutex                   = sync.Mutex{}
		mmap                        = make(map[string][]string)
	)
	// 创建临时文件
	tmpOfile, err := os.Create(task.OutputFile + ".tmp")
	if err != nil {
		return err
	}
	// 获取reduce任务数量
	nMap, nReduce, err := getTaskCount()
	if err != nil {
		return err
	}
	// 获取中间文件以及最终数量（可能因为map error而变小）
	safeGoroutine(func() {
		for {
			files, finalCount, err := getIntermediateFiles(task, receiveIntermediateCount.Load())
			if err != nil {
				break
			}
			for _, file := range files {
				intermediateFiles <- file
				receiveIntermediateCount.Add(1)
			}
			if receiveIntermediateCount.Load() >= finalCount {
				finalIntermediateCount.Store(finalCount)
				close(intermediateFiles)
				break
			}
			time.Sleep(time.Second)
		}
	}, nil)
	// 处理中间文件
	for intermediateFile := range intermediateFiles {
		maxHandleIntermediateCount <- struct{}{}
		intermediateFile := intermediateFile
		safeGoroutine(func() {
			defer func() {
				<-maxHandleIntermediateCount
			}()
			defer hasHandleIntermediateNumber.Add(1)
			// 读取中间文件
			bytes, err := os.ReadFile(intermediateFile)
			if err != nil {
				return
			}
			var keyValues []KeyValue
			if err = json.Unmarshal(bytes, &keyValues); err != nil {
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
		}, func(panic interface{}) {
			defer func() {
				<-maxHandleIntermediateCount
			}()
			defer hasHandleIntermediateNumber.Add(1)
		})
	}
	for hasHandleIntermediateNumber.Load() != finalIntermediateCount.Load() {
		time.Sleep(time.Second)
	}
	// 将output内容写入output文件文件中
	results := make([]KeyValue, 0)
	for k, vs := range mmap {
		results = append(results, KeyValue{k, reducef(k, vs)})
	}
	sort.Sort(ByKey(results))
	for _, r := range results {
		if _, err = fmt.Fprintf(tmpOfile, "%v %v\n", r.Key, r.Value); err != nil {
			return err
		}
	}
	// 将文件重命名
	if err = os.Rename(task.OutputFile+".tmp", task.OutputFile); err != nil {
		return err
	}
	return err
}

func getTaskCount() (int, int, error) {
	args := GetNTaskArgs{
		WorkerId: workerId,
	}
	reply := GetNTaskReply{}
	if err := client.Call("Coordinator.GetNTask", &args, &reply); err != nil {
		return 0, 0, err
	}
	nReduce := reply.NReduce
	nMap := reply.NMap
	return nMap, nReduce, nil
}

func getIntermediateFiles(task Task, receiveIntermediateCount int64) ([]string, int64, error) {
	args := GetIntermediateFilesArgs{
		WorkerId:     workerId,
		ReduceTaskId: task.Id,
		ReceiveCount: receiveIntermediateCount,
	}
	reply := GetIntermediateFilesReply{}
	if err := client.Call("Coordinator.GetIntermediateFiles", &args, &reply); err != nil {
		return nil, 0, err
	}
	return reply.IntermediateFiles, reply.FinalCount, nil
}

func askTask() (Task, error) {
	args := AskTaskArgs{WorkerId: workerId}
	reply := AskTaskReply{}
	if err := client.Call("Coordinator.AskTask", &args, &reply); err != nil {
		return Task{}, err
	}
	return reply.Task, nil
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

func reportError(client *rpc.Client, workerId string, taskType string, taskId int, err interface{}) {
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
