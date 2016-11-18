/* api_server.go - api server */
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
api_server handles rpc calls from users, e.g., create a new task
*/
package api_server

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

import (
	"www.baidu.com/golang-lib/log"
)

import (
	"controller_conf"
)

type ApiServer struct {
	httpPort  int // rpc server listening port
}
/*
type ConfServer struct {
	confPort  int
}
type StateServer struct {
	statePort int
}
*/
// use singleton model
var apiServer *ApiServer
//var confServer *ConfServer
//var stateServer *StateServer
/*
func Init(conf controller_conf.ControllerConfig) {
	apiServer = newApiServer()
	apiServer.httpPort = conf.Main.HttpPort
	confServer = newConfServer()
	confServer.confPort = conf.Main.ConfPort
	stateServer = newStateServer()
	stateServer.statePort = conf.Main.StatePort
}*/

func newApiServer() *ApiServer {
	server := new(ApiServer)
	return server
}
/*
func newConfServer() *ConfServer {
	server := new(ConfServer)
	return server
}
func newStateServer() *StateServer {
	server := new(StateServer)
	return server
}
*/
func Start(conf controller_conf.ControllerConfig) {
	apiServer = newApiServer()
	apiServer.httpPort = conf.Main.HttpPort
/*	confServer = newConfServer()
	confServer.confPort = conf.Main.ConfPort
	stateServer = newStateServer()
	stateServer.statePort = conf.Main.StatePort
	log.Logger.Info("api_server starts!!! httpPort:%d, ConfPort:%d, StatePort:%d",apiServer.httpPort,confServer.confPort,stateServer.statePort)
*/	
	go func() {
		log.Logger.Info("api_server starts")
		rpc.Register(apiServer)
		log.Logger.Info("api_server register finish")
		rpc.HandleHTTP()
		log.Logger.Info("listening httpPort:%d", apiServer.httpPort)
		listener, e := net.Listen("tcp", ":"+fmt.Sprintf("%d", apiServer.httpPort))
		if e != nil {
			log.Logger.Error("err in listening httpPort error:%s", e.Error())
			return
		}
		http.Serve(listener, nil)
	}()
	/*
	go func() {
		log.Logger.Info("conf_server starts")
		rpc.Register(confServer)
		rpc.HandleHTTP()
		log.Logger.Info("listening ConfPort:%d", confServer.confPort)
		Conflistener, e := net.Listen("tcp", ":"+fmt.Sprintf("%d", confServer.confPort))
		if e != nil {
			log.Logger.Error("err in listening ConfPort error:%s", e.Error())
			return
		}
		http.Serve(Conflistener, nil)
	}()
	go func() {
		log.Logger.Info("state_server starts")
		rpc.Register(stateServer)
		rpc.HandleHTTP()
		log.Logger.Info("listening StatePort:%d", stateServer.statePort)
		Statelistener, e := net.Listen("tcp", ":"+fmt.Sprintf("%d", stateServer.statePort))
		if e != nil {
			log.Logger.Error("err in listening StatePort error:%s", e.Error())
			return
		}
		http.Serve(Statelistener, nil)
	}()
	*/
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Logger.Error("http error is: %+v", err)
			}
		}()
		log.Logger.Info("Server's FileServer starts")
		http.Handle("/", http.FileServer(http.Dir("./")))
//		http.Handle("/files", http.StripPrefix("/files", http.FileServer(http.Dir("./"))))
		err := http.ListenAndServe(":8067", nil)
		if err != nil {
			log.Logger.Error("http listen and serve err: %s", err.Error())
		}
	}()
}
