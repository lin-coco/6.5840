# README.md

https://pdos.csail.mit.edu/6.824/schedule.html

## Lab 1: MapReduce

论文：http://blog.mrcroxx.com/posts/paper-reading/mapreduce-osdi04/

> Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and 
> the worker. There will be just one coordinator process, and one or more worker processes executing 
> in parallel. In a real system the workers would run on a bunch of different machines, but for this lab
> you'll run them all on a single machine. The workers will talk to the coordinator via RPC. Each worker
> process will ask the coordinator for a task, read the task's input from one or more files, execute the 
> task, and write the task's output to one or more files. The coordinator should notice if a worker hasn't
> completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same 
> task to a different worker.

任务：

1. 分布式系统
2. 协调者（一个进程）、工作者（一个或一个以上）
3. 工作者并行执行
4. 协调者与工作者通过RPC通信
5. 工作者向协调者询问任务，从一个或多个文件读取输入，执行任务，输出结构到一个或多个文件
6. 如果工作者超时或者没有规定时间完成任务，协调者将任务给其他的工作者（任务副本）

运行协调者

```bash
go run mrcoordinator.go pg-*.txt
```

运行工作者

```bash
go run mrworker.go wc.so
```

整理结果

```bash
cat mr-out-* | sort | more
```

协调者是服务器

工作者是客户端





