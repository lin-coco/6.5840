package test

import (
	"fmt"
	"testing"
	"time"
)

/*
defer recover只针对当前的协程
*/
func TestDefer(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("第一个defer 捕捉到了panic")
		}
		//fmt.Println("第一个defer")
	}()

	for i := 0; i < 10; i++ {
		go func() {
			fmt.Println("第二个defer")
			//time.Sleep(time.Second)
			panic("panic")
		}()
	}
	time.Sleep(5)
}
