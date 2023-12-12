package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
		// 已派发任务
		Tasks map[int]Task
		// map任务 中间文件写入完成顺序
		CompletedIntermediateFiles []string
		// reduce任务 结果写入完成顺序
		CompletedOutputFiles []string
		ErrorTask            []Task
		CompleteTask         []Task
		// 记录 reduce 数量
		NReduce int
		// 记录 map 数量
		NMap int
		// 结束标识
		done bool
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
	completeTaskMutex      = sync.Mutex{}
	errorTaskMutex         = sync.Mutex{}
	intermediateFilesMutex = sync.Mutex{}
	outputFilesMutex       = sync.Mutex{}
	IntermediateDir        = "intermediate"     // 中间目录
	IntermediatePrefix     = "mr-intermediate-" // 中间目录
	OutputDir              = ""                 // 输出目录        // 输出目录
	OutputPrefix           = "mr-out-"          // 输出目录        // 输出目录
)

/*
AskTask 工作者向协调者询问任务
要保证并发安全
*/
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	err := func() error {
		taskMutex.Lock()
		defer taskMutex.Unlock()
		if len(c.UnDistributedTasks) == 0 {
			return nil
		}
		var (
			id   int
			task Task
		)
		for id, task = range c.UnDistributedTasks {
			log.Printf("distribute task:%d to worker:%s", task.Id, args.WorkerId)
			break
		}
		task.WorkerId = args.WorkerId
		delete(c.UnDistributedTasks, id)
		c.Tasks[id] = task
		reply.Task = task
		return nil
	}()
	if err != nil {
		return err
	}
	c.pingTaskState(reply.Task)
	return nil
}

func (c *Coordinator) pingTaskState(task Task) {
	if task.Type == "" {
		return
	}
	go func(task Task) {
		lastTask := task
		for {
			time.Sleep(10 * time.Second)
			if b := func() bool {
				taskMutex.Lock()
				defer taskMutex.Unlock()
				newTask := c.Tasks[lastTask.Id]
				if newTask.WorkerId != lastTask.WorkerId {
					return true
				}
				if newTask.State == lastTask.State {
					log.Printf("a worker:%s hasn't completed its task:%d in a reasonable amount of time\n", task.WorkerId, task.Id)
					// 放到未分配任务中
					newTask.WorkerId = ""
					newTask.State = ""
					c.UnDistributedTasks[newTask.Id] = newTask
					delete(c.Tasks, newTask.Id)
					return true
				}
				lastTask = newTask
				return false
			}(); b {
				break
			}
		}
	}(task)
}

/*
ReportTaskState 工作者向协调者报告状态
*/
func (c *Coordinator) ReportTaskState(args *ReportTaskStateArgs, reply *ReportTaskStateReply) error {
	if err := func() error {
		taskMutex.Lock()
		defer taskMutex.Unlock()
		if t, ok := c.Tasks[args.Id]; !ok {
			return fmt.Errorf("task:%d may be wait a different worker", t.Id)
		} else if args.WorkerId != t.WorkerId {
			return fmt.Errorf("task:%d may be give a different worker:%s", t.Id, t.WorkerId)
		} else {
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
				log.Printf("task:%d has idle\n", t.Id)
			case PROGRESS:
				log.Printf("task:%d has progress\n", t.Id)
			case COMPLETED:
				log.Printf("task:%d has completed\n", t.Id)
				if t.Type == MAP {
					intermediateFilesMutex.Lock()
					c.CompletedIntermediateFiles = append(c.CompletedIntermediateFiles, t.IntermediateFile)
					intermediateFilesMutex.Unlock()
				}
				if t.Type == REDUCE {
					outputFilesMutex.Lock()
					c.CompletedOutputFiles = append(c.CompletedOutputFiles, t.OutputFile)
					outputFilesMutex.Unlock()
				}
				completeTaskMutex.Lock()
				c.CompleteTask = append(c.CompleteTask, t)
				completeTaskMutex.Unlock()
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	// 如果全部完成，则结束
	go c.printTaskState()
	reply.Status = http.StatusOK
	return nil
}

/*
GetIntermediateFiles reduce工作者定期获取已完成的中间文件
*/
func (c *Coordinator) GetIntermediateFiles(args *GetIntermediateFilesArgs, reply *GetIntermediateFilesReply) error {
	errorTaskMutex.Lock()
	defer errorTaskMutex.Unlock()
	intermediateFilesMutex.Lock()
	defer intermediateFilesMutex.Unlock()
	reply.IntermediateFiles = c.CompletedIntermediateFiles[args.ReceiveCount:len(c.CompletedIntermediateFiles)]
	reply.FinalCount = int64(len(c.Splits))
	for _, t := range c.ErrorTask {
		if t.Type == MAP {
			reply.FinalCount--
		}
	}
	return nil
}

/*
GetNTask reduce工作者获取NReduce
*/
func (c *Coordinator) GetNTask(args *GetNTaskArgs, reply *GetNTaskReply) error {
	reply.NReduce = c.NReduce
	reply.NMap = c.NMap
	return nil
}

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
	log.Println("server running in:", ":1234")
}

func (c *Coordinator) Done() bool {
	return c.done
}

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
			Type:             MAP,
			Split:            split,
			IntermediateFile: filepath.Join(IntermediateDir, IntermediatePrefix+strconv.Itoa(taskIdIncr)),
		}
		taskIdIncr++
	}
	c.NMap = len(c.Splits)
	for i := 0; i < nReduce; i++ {
		unDistributedTasks[taskIdIncr] = Task{
			Id:         taskIdIncr,
			Type:       REDUCE,
			OutputFile: filepath.Join(OutputDir, OutputPrefix+strconv.Itoa(taskIdIncr)),
		}
		taskIdIncr++
	}
	c.UnDistributedTasks = unDistributedTasks

	if err := os.RemoveAll(IntermediateDir); err != nil {
		log.Fatal("remove all IntermediateDir error:", err)
	}
	if err := os.MkdirAll(IntermediateDir, os.ModePerm); err != nil {
		log.Fatal("mkdir all IntermediateDir error:", err)
	}
	_ = exec.Command("sh", "-c", "rm -f mr-out-*").Run()
	c.server()
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.printTaskState()
		}
	}()
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
	return &c
}

func (c *Coordinator) printTaskState() {
	taskMutex.Lock()
	defer taskMutex.Unlock()
	completeTaskMutex.Lock()
	defer completeTaskMutex.Unlock()
	errorTaskMutex.Lock()
	defer errorTaskMutex.Unlock()
	log.Printf("UnDistributedTasks num:%d, Task num:%d, CompleteTask num:%d, ErrorTask num:%d\n", len(c.UnDistributedTasks), len(c.Tasks), len(c.CompleteTask), len(c.ErrorTask))
	if len(c.ErrorTask)+len(c.CompleteTask) >= len(c.Splits)+c.NReduce {
		log.Printf("all task has ended\n\n")
		log.Println("CompleteTask", c.CompleteTask)
		log.Println("ErrorTask", c.ErrorTask)
		log.Println("CompletedOutputFiles", c.CompletedOutputFiles)
		c.done = true
	}
}
