package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	/*
		任务状态
	*/
	IDLE      = "idle"        // 等待中
	PROGRESS  = "in-progress" // 进行中
	ERROR     = "error"
	COMPLETED = "completed" // 已完成
	/*
		任务类型
	*/
	MAP    = "map"
	REDUCE = "reduce"
)

type (
	Coordinator struct {
		// 读取的文件
		InputFiles []string
		// 分区
		Splits []string
		// 未派发任务
		UnDistributedTasks map[int]Task
		// 任务
		Tasks map[int]Task
		// map任务 中间文件写入完成顺序
		CompletedIntermediateFiles []string
		// reduce任务 结果写入完成顺序
		CompletedOutputFiles []string
		ErrorTask            []Task
		CompleteTask         []Task
		// 使用 reduce worker数量
		NReduce int
	}
	Task struct {
		// 共同字段
		Id       int    // 任务id
		Type     string // 类型
		WorkerId string // 工作者id
		State    string // 状态
		// map类型任务字段
		Split            string // 分区
		IntermediateFile string // 中间文件
		// reduce类型任务字段
		OutputFile string // 输出文件
	}
)

var (
	taskMutex              = sync.Mutex{}
	intermediateFilesMutex = sync.Mutex{}
	outputFilesMutex       = sync.Mutex{}
	errorTaskMutex         = sync.Mutex{}
	completeTaskMutex      = sync.Mutex{}
	IntermediateDir        = "intermediate"     // 中间目录
	IntermediatePrefix     = "mr-intermediate-" // 中间目录
	OutputDir              = "output"           // 输出目录        // 输出目录
	OutputPrefix           = "mr-out-"          // 输出目录        // 输出目录
	HealthCheckTimeout     = 10
)

/*
AskTask 工作者向协调者询问任务
要保证并发安全
*/
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	taskMutex.Lock()
	defer taskMutex.Unlock()
	if len(c.UnDistributedTasks) == 0 {
		return nil
	}
	defer c.calTaskState(reply.Task)
	var (
		id   int
		task Task
	)
	for id, task = range c.UnDistributedTasks {
		break
	}
	task.WorkerId = args.WorkerId
	delete(c.UnDistributedTasks, id)
	c.Tasks[id] = task
	reply.Task = task
	return nil
}

func (c *Coordinator) calTaskState(task Task) {
	if task.Type == "" {
		return
	}
	go func(lastTask Task) {
		for {
			time.Sleep(10 * time.Second)
			newTask := c.Tasks[lastTask.Id]
			if newTask.State == COMPLETED || newTask.State == ERROR || newTask.WorkerId != lastTask.WorkerId {
				break
			}
			if newTask.State == lastTask.State {
				log.Printf("a worker:%s hasn't completed its task in a reasonable amount of time\n", task.WorkerId)
				// 放到未分配任务中
				taskMutex.Lock()
				newTask.WorkerId = ""
				newTask.State = ""
				c.UnDistributedTasks[newTask.Id] = newTask
				delete(c.Tasks, newTask.Id)
				taskMutex.Unlock()
				break
			}
		}
	}(task)
}

/*
ReportTaskState 工作者向协调者报告状态
*/
func (c *Coordinator) ReportTaskState(args *ReportTaskStateArgs, reply *ReportTaskStateReply) error {
	if t, ok := c.Tasks[args.Id]; !ok {
		return fmt.Errorf("task:%d may be wait a different worker", t.Id)
	} else if args.WorkerId != t.WorkerId {
		return fmt.Errorf("task:%d may be give a different worker:%s", t.Id, t.WorkerId)
	} else {
		taskMutex.Lock()
		t.State = args.State
		delete(c.Tasks, args.Id)
		c.Tasks[args.Id] = t
		switch t.State {
		case ERROR:
			log.Printf("task:%d has error:%v\n", t.Id, args.Err)
			errorTaskMutex.Lock()
			c.ErrorTask = append(c.ErrorTask, t)
			errorTaskMutex.Unlock()
		case IDLE:
		case PROGRESS:
		case COMPLETED:
			if t.Type == MAP {
				intermediateFilesMutex.Lock()
				c.CompletedIntermediateFiles = append(c.CompletedIntermediateFiles, t.IntermediateFile)
				intermediateFilesMutex.Unlock()
			}
			if t.Type == REDUCE {
				outputFilesMutex.Lock()
				c.CompletedOutputFiles = append(c.CompletedOutputFiles, t.IntermediateFile)
				outputFilesMutex.Unlock()
			}
			completeTaskMutex.Lock()
			c.CompleteTask = append(c.CompleteTask, t)
			completeTaskMutex.Unlock()
		}

		taskMutex.Unlock()
	}
	// 如果全部完成，则结束
	if len(c.ErrorTask)+len(c.CompleteTask) == len(c.Splits)+c.NReduce {
		_ = os.RemoveAll(IntermediateDir)
		log.Printf("\nall task has ended\n")
		log.Println(c.CompleteTask)
		log.Println(c.ErrorTask)
		log.Println(c.CompletedOutputFiles)
		c.Done()
	}
	reply.Status = http.StatusOK
	return nil
}

/*
GetIntermediateFiles reduce工作者定期获取已完成的中间文件
*/
func (c *Coordinator) GetIntermediateFiles(args *GetIntermediateFilesArgs, reply *GetIntermediateFilesReply) error {
	intermediateFilesMutex.Lock()
	defer intermediateFilesMutex.Unlock()
	errorTaskMutex.Lock()
	defer errorTaskMutex.Unlock()
	reply.IntermediateFiles = c.CompletedIntermediateFiles[args.ReceiveCount:len(c.CompletedIntermediateFiles)]
	reply.Sum = len(c.Splits)
	for _, t := range c.ErrorTask {
		if t.Type == MAP {
			reply.Sum--
		}
	}
	return nil
}

/*
GetNReduce reduce工作者获取NReduce
*/
func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.NReduce
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("rpc register error:", err)
	}
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	os.Exit(1)
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles: files,
		Splits:     files,
		Tasks:      make(map[int]Task, len(files)),
		NReduce:    nReduce,
	}
	taskIdIncr := 0
	unDistributedTasks := make(map[int]Task, len(c.Splits)+nReduce)

	for _, split := range c.Splits {
		unDistributedTasks[taskIdIncr] = Task{
			Id:               taskIdIncr,
			Split:            split,
			IntermediateFile: filepath.Join(IntermediateDir, IntermediatePrefix+strconv.Itoa(taskIdIncr)),
		}
		taskIdIncr++
	}
	for i := 0; i < nReduce; i++ {
		unDistributedTasks[taskIdIncr] = Task{
			Id:         taskIdIncr,
			OutputFile: filepath.Join(OutputDir, OutputPrefix+strconv.Itoa(taskIdIncr)),
		}
		taskIdIncr++
	}
	c.UnDistributedTasks = unDistributedTasks

	if err := os.RemoveAll(IntermediateDir); err != nil {
		log.Fatal("remove all IntermediateDir error:", err)
	}
	if err := os.MkdirAll(IntermediateDir, 755); err != nil {
		log.Fatal("mkdir all IntermediateDir error:", err)
	}
	c.server()
	return &c
}
