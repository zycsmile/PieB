/* dt-contoller.go - main program of dt-controller   */
/*
modification history
--------------------
2015/07/22, by Guang Yao, create
*/
/*
DESCRIPTION
main func
*/
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"
)

import (
	"code.google.com/p/log4go"
	"www.baidu.com/golang-lib/log"
)

import (
//	"agent_state_collector"
	"api_server"
	"controller_conf"
	"scheduler"
	"state_manager"
)

var (
	help     *bool   = flag.Bool("h", false, "to show help")
	confRoot *string = flag.String("c", "../../conf_test", "root path of config file")
	logPath  *string = flag.String("l", "./log", "dir path of log")
	stdOut   *bool   = flag.Bool("s", false, "to show log in stdout")
	showVer  *bool   = flag.Bool("v", false, "to show version")
	debugLog *bool   = flag.Bool("d", false, "to show debug log (otherwise >= info)")
)

func Exit(code int) {
	log.Logger.Close()
	/* to overcome bug in log, sleep for a while    */
	time.Sleep(1 * time.Second)
	os.Exit(code)
}

func main() {
	var err error
	var logSwitch string

	//TODO: to remove
	version := "1.0.0.0"

	flag.Parse()
	if *help {
		flag.PrintDefaults()
		return
	}
	if *showVer {
		fmt.Printf("dt-controller: version %s\n", version)
		return
	}

	// debug switch
	if *debugLog {
		logSwitch = "DEBUG"
	} else {
		logSwitch = "INFO"
	}

	// initialize log
	// set log buffer size
	log4go.SetLogBufferLength(100)
	// if blocking, log will be dropped
	log4go.SetLogWithBlocking(false)
	// we want to get state of log4go
	log4go.SetWithModuleState(true)

	err = log.Init("dt-controller", logSwitch, *logPath, *stdOut, "midnight", 5)
	if err != nil {
		fmt.Printf("dt-controller: err in log.Init():%s\n", err.Error())
		Exit(1)
	}

	log.Logger.Info("dt-controller[version:%s] start", version)

	// load config
	confPath := path.Join(*confRoot, "config.conf")
	config, err := controller_conf.ConfigLoad(confPath, *confRoot)
	if err != nil {
		log.Logger.Error("main():err in ConfigLoad():%s", err.Error())
		Exit(1)
	}

	// set number of max cpus to use
	runtime.GOMAXPROCS(config.Main.MaxCpus)

	// init state manager
	state_manager.Init()

	// init scheduler
	scheduler.Init(config)

	// create api-server
	//api_server.Init(config)

	// create agent_state_collector
	//agent_state_collector.Init()

	// start the routines
	scheduler.Start()
	api_server.Start(config)
	//agent_state_collector.Start()

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	// ensure that all logs are export and normal exit
	Exit(0)
}
