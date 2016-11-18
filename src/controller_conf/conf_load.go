/* conf_load.go: load conf for dt-controller */
/*
modification history
--------------------
2015/07/22, by Guang Yao, create
*/
/*
DESCRIPTION
*/

package controller_conf

import (
	"fmt"
)

import (
	"code.google.com/p/gcfg"
)

type ControllerConfig struct {
	Main   ConfigMain
	Output ConfigOutput
}

type ConfigMain struct {
	HttpPort        int    // http port of API server
	ConfPort        int
	StatePort       int
	MonitorPort     int    // http port for monitor and reload
	MaxCpus         int    // max number of CPUs to use
	MonitorInterval int    // interval for get diff of state; in secs
	ScheduleCycle   int64  // cycle of schedule; in secs
	ElectionServer  string // election server
}

type ConfigOutput struct {
	AgentMonitorUrls []string // agent monitor urls
}

/*
LoadConfig - load config file for controller

Params:
    - filePath: path of config file
    - confRoot: root path of config

Returns:
    (ControllerConfig, error)
*/
func ConfigLoad(filePath string, confRoot string) (ControllerConfig, error) {
	var cfg ControllerConfig
	var err error

	// read config from file
	err = gcfg.ReadFileInto(&cfg, filePath)
	if err != nil {
		return cfg, err
	}

	// check main conf
	if err = cfg.Main.Check(confRoot); err != nil {
		return cfg, err
	}

	// check output conf
	if err = cfg.Output.Check(confRoot); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// check main conf
func (cfg *ConfigMain) Check(confRoot string) error {
	// check HttpPort
	if cfg.HttpPort < 9000 || cfg.HttpPort >= 10000 {
		return fmt.Errorf("HttpPort[%d] must be in range [9000, 10000)", cfg.HttpPort)
	}

	// check MonitorPort
	if cfg.MonitorPort < 9000 || cfg.MonitorPort >= 10000 {
		return fmt.Errorf("MonitorPort[%d] must be in range [9000, 10000)", cfg.MonitorPort)
	}

	// check MaxCpus
	if cfg.MaxCpus <= 0 {
		return fmt.Errorf("MaxCpus[%d] must be larger than 0", cfg.MaxCpus)
	}

	// check MonitorInterval
	if cfg.MonitorInterval <= 0 {
		return fmt.Errorf("MonitorInterval[%d] must be larger than 0", cfg.MonitorInterval)
	}

	// check ScheduleCycle
	if cfg.ScheduleCycle <= 0 {
		return fmt.Errorf("ScheduleCycle[%d] must be larger than 0", cfg.ScheduleCycle)
	}

	// check ElectionServer
	// TODO: allow nil currently;should be revised

	return nil
}

// check output conf
func (cfg *ConfigOutput) Check(confRoot string) error {
	//check AgentMonitorUrls
	// TODO: allow nil currently;should be revised

	return nil
}
