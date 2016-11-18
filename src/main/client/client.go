/* client.go - client for test api server */
/*
modification history
--------------------
2015/7/22, by Guang Yao, create
*/
/*
DESCRIPTION
*/

package main

import (
	"fmt"
	"net/rpc"
	"time"
//    "io"
    "os"
    "bufio"
    "strings"
)

import (
    "www.baidu.com/golang-lib/log"
    "strconv"
)

type SubmitTaskRequest struct {
	SrcUrl       string
	DstHostname  string
	Deadline     time.Time
	Mode         int
	Cmd          int
	Priority     int
	Product      string
	IsSensitive  bool
	IsCompressed bool
	TimeToLive   time.Time
}

type SubmitTaskResponse struct {
	RetCode int // 0: OK; non-0 otherwise
	ErrMsg  string
}

func check(e error){
    if e != nil {
        panic(e)
    }
}

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:9990")
	if err != nil {
		fmt.Printf("err in connect server:%s \n\n", err.Error())
	}

	// Synchronous call
	request := &SubmitTaskRequest{
		SrcUrl:       "http://yf-l2-bfetest06.yf01.baidu.com",
		DstHostname:  "sh01-mco-hunbuwise1645.sh01",
		Deadline:     time.Now().Add(time.Hour),
		Mode:         1,
		Cmd:          1,
		Priority:     1,
		Product:      "psop",
		IsSensitive:  true,
		IsCompressed: true,
		TimeToLive:   time.Now().Add(time.Hour * 24),
	}
	response := new(SubmitTaskResponse)

    f, err := os.Create("../test.txt")
//    f, err := os.Create("/home/work/zhangyuchao02/controller/main/TCT.txt")
    check(err)
    defer f.Close()
//    start_time := time.Now().Unix()
    timestamp := time.Now().Unix()
    tm := time.Unix(timestamp,0)
    tt := tm.Format("2006-01-02 15:04:05")
    n1,err := f.WriteString(tt +"\n")
    check(err)
    log.Logger.Info("write time to file %d letters: %s\n", n1,tt)
    f.Sync()

    fmt.Printf("Begin at %v\n", time.Now())

    f2, err := os.Open("dst.txt")
    check(err)
    buf := bufio.NewReader(f2)
    for i:=0;i<4;i++ {
        line, err := buf.ReadString('\n')
        check(err)
        line = strings.TrimSpace(line)
//        request.DstHostname = line
        for j:=1;j<=2;j++{
            str1 := "http://test"
            str2 := ".yf.baidu.com/data"
            str := strconv.Itoa(j)
            source := str1+str+str2
            request.SrcUrl = source
            str3 := "test"
            str4 := "copy"
            request.DstHostname = str3+str+line
            err = client.Call("ApiServer.SubmitTask", request, response)
            if err != nil {
                fmt.Printf("err in call ApiServer.SubmitTask %s:%s \n\n", request.DstHostname, err.Error())
            }
            fmt.Printf("ApiServer.SubmitTask %s, from %s: state:%d, errMsg:%s \n\n", request.DstHostname, request.SrcUrl, response.RetCode, response.ErrMsg)

            request.DstHostname = str4+str+line
            err = client.Call("ApiServer.SubmitTask", request, response)
            if err != nil {
                fmt.Printf("err in call ApiServer.SubmitTask %s:%s \n\n", request.DstHostname, err.Error())
            }
            fmt.Printf("ApiServer.SubmitTask %s, from %s: state:%d, errMsg:%s \n\n", request.DstHostname, request.SrcUrl, response.RetCode, response.ErrMsg)
        }
    }
}
