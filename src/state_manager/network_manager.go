/* network_manager.go - manager of network*/
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package state_manager

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

import (
	"www.baidu.com/golang-lib/log"
)

import (
	. "types"
)

const (
	IdcDataPath        = "../../data/idc.data"
	IdcLinkDataPath    = "../../data/idc_link.data"
	RegionLinkDataPath = "../../data/region_link.data"
)

type NetworkManager struct {
	idcLock sync.RWMutex
	idcs    map[string]*Idc // idc name => idc

	regionLock sync.RWMutex
	regions    map[string]*Region // region name => region

	idcLinkLock sync.RWMutex
	idcLinks    map[string]*IdcLink // link name => IDCLink

	regionLinkLock sync.RWMutex
	regionLinks    map[string]*RegionLink // link name => regionLink
}

func NewNetworkManager() (*NetworkManager, error) {
	m := new(NetworkManager)

	err := m.init()
	if err != nil {
		log.Logger.Error("err in init network manager:%s", err.Error())
		return nil, err
	}

	return m, nil
}

func (m *NetworkManager) init() error {

	err := m.initIdcAndRegion()
	if err != nil {
		return err
	}

	err = m.initIdcLinks()
	if err != nil {
		return err
	}

	err = m.initRegionLinks()
	if err != nil {
		return err
	}

	return nil
}

func (m *NetworkManager) addIdc(idc *Idc) {
	m.idcLock.Lock()
	defer m.idcLock.Unlock()

	m.idcs[idc.IdcName] = idc
}

func (m *NetworkManager) addRegion(region *Region) {
	m.regionLock.Lock()
	defer m.regionLock.Unlock()

	m.regions[region.RegionName] = region
}

func (m *NetworkManager) initIdcAndRegion() error {
	m.idcs = make(map[string]*Idc)
	m.regions = make(map[string]*Region)

	// load idc data file
	idcDataFile, err := os.Open(IdcDataPath)
	defer idcDataFile.Close()
	if err != nil {
		log.Logger.Error(err.Error())
		return err
	}

	// read the idc data file
	scanner := bufio.NewScanner(idcDataFile)
	for scanner.Scan() {
		//# format:
		//domain: idc, idc, ...
		idcList := scanner.Text()
		if idcList[0] == '#' {
			continue
		}

		// split the line
		idcListItems := strings.Split(idcList, ": ")
		if len(idcListItems) != 2 {
			return fmt.Errorf("error formatted idc data file: %d", idcList)
		}

		// add the region
		regionName := idcListItems[0]
		idcs := make(map[string]*Idc)
		region := &Region{regionName, idcs}
		m.addRegion(region)

		// add the idcs
		idcNames := strings.Split(idcListItems[1], ", ")
		for _, idcName := range idcNames {
			idc := new(Idc)
			idc.IdcName = idcName
			idc.Region = region

			region.Idcs[idcName] = idc
			m.addIdc(idc)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Logger.Error(err.Error())
		return err
	}

	return nil
}

func (m *NetworkManager) initIdcLinks() error {
	m.idcLinks = make(map[string]*IdcLink)

	// load idc data file
	idcLinkDataFile, err := os.Open(IdcLinkDataPath)
	defer idcLinkDataFile.Close()
	if err != nil {
		log.Logger.Error(err.Error())
		return err
	}

	// read the idc link data file
	scanner := bufio.NewScanner(idcLinkDataFile)
	for scanner.Scan() {
		//# format:
		//src idc name, dst idc name, in quota(in Gbps), out quota(in Gbps)
		idcLinkStr := scanner.Text()
		if idcLinkStr[0] == '#' {
			continue
		}

		// create the link
		idcLinkItems := strings.Split(idcLinkStr, ", ")
		srcIdcname := idcLinkItems[0]
		dstIdcname := idcLinkItems[1]
		srcIdc := m.getIdcByName(srcIdcname)
		if srcIdc == nil {
			return fmt.Errorf("unknown idc: %s", srcIdcname)
		}
		dstIdc := m.getIdcByName(dstIdcname)
		if dstIdc == nil {
			return fmt.Errorf("unknown idc: %s", dstIdcname)
		}
		fromLinkName := srcIdcname + "-" + dstIdcname
		toLinkName := dstIdcname + "-" + srcIdcname
		fromLink := IdcLink{srcIdc, dstIdc, fromLinkName, IDC_LINK_STATUS_UP, 0, 0}
		toLink := IdcLink{dstIdc, srcIdc, toLinkName, IDC_LINK_STATUS_UP, 0, 0}

		// set quota
		fromQuota, err := strconv.ParseInt(idcLinkItems[2], 10, 0)
		if err != nil {
			return err
		}
		fromLink.Quota = fromQuota * 1024 * 1024 / 8 // from Gbps => KB/s
		toQuota, err := strconv.ParseInt(idcLinkItems[3], 10, 0)
		if err != nil {
			return err
		}
		toLink.Quota = toQuota * 1024 * 1024 / 8 // from Gbps => KB/s
        //added by zyc 20160222
        // set Free
        fromLink.Free = fromLink.Quota
        toLink.Free = toLink.Quota

		// add to map
		m.idcLinks[fromLinkName] = &fromLink
		m.idcLinks[toLinkName] = &toLink
	}

	if err := scanner.Err(); err != nil {
		log.Logger.Error(err.Error())
		return err
	}

	return nil
}

func (m *NetworkManager) initRegionLinks() error {
	m.regionLinks = make(map[string]*RegionLink)

	// load region link data file
	regionLinkDataFile, err := os.Open(RegionLinkDataPath)
	defer regionLinkDataFile.Close()
	if err != nil {
		log.Logger.Error(err.Error())
		return err
	}

	// read the region link data file
	scanner := bufio.NewScanner(regionLinkDataFile)
	for scanner.Scan() {
		//# format:
		//idcname, in quota(in Gbps), out quota(in Gbps)
		regionLinkStr := scanner.Text()
		if regionLinkStr[0] == '#' {
			continue
		}

		// create the link
		regionLinkItems := strings.Split(regionLinkStr, ",")
		srcRegionName := regionLinkItems[0]
		dstRegionName := regionLinkItems[1]
		srcRegion := m.getRegionByName(srcRegionName)
		if srcRegion == nil {
			return fmt.Errorf("invalid region name %s", srcRegionName)
		}
		dstRegion := m.getRegionByName(dstRegionName)
		if dstRegion == nil {
			return fmt.Errorf("invalid region name %s", dstRegionName)
		}
		linkName := srcRegionName + "-" + dstRegionName
		link := RegionLink{srcRegion, dstRegion, linkName, 0, nil}

		// set quota
		quota, err := strconv.ParseInt(regionLinkItems[2], 10, 0)
		if err != nil {
			return err
		}
		link.Quota = quota

		// add to table
		m.regionLinks[linkName] = &link
	}

	if err := scanner.Err(); err != nil {
		log.Logger.Error(err.Error())
		return err
	}

	return nil
}

func (m *NetworkManager) getIdcByName(idcName string) *Idc {
	m.idcLock.RLock()
	defer m.idcLock.RUnlock()

	idc, _ := m.idcs[idcName]

	return idc
}

func (m *NetworkManager) getRegionByName(regionName string) *Region {
	m.regionLock.RLock()
	defer m.regionLock.RUnlock()

	region, _ := m.regions[regionName]

	return region
}

func parseIdcFromAgentname(agentName string, agentType AgentType) *Idc {
	m := stateManager.networkManager

	switch agentType {
	case AGENT_TYPE_HDFS, AGENT_TYPE_NFS:
		// format: yq01-wutai-hdfs.dmop.baidu.com
		agentNameStrs := strings.Split(agentName, "-")
		return m.getIdcByName(agentNameStrs[0])
	case AGENT_TYPE_HOST:
		// format: yf-op-bfe-test00.yf01.baidu.com
		agentNameStrs := strings.Split(agentName, ".")
		if len(agentNameStrs) >= 1 { //edit in 20160520: >=2 ==> >=1 by zyc
			return m.getIdcByName(agentNameStrs[1])
		}
		return nil
	default:
		return nil
	}
}

func (m *NetworkManager) getAllIdcLinks() []*IdcLink {
	links := make([]*IdcLink, 0)
	for _, link := range m.idcLinks {
		links = append(links, link)
	}

	return links
}

func (m *NetworkManager) getAllRegionLinks() []*RegionLink {
	links := make([]*RegionLink, 0)
	for _, link := range m.regionLinks {
		links = append(links, link)
	}

	return links
}

func GetAgentsDistanceType(dstAgent *Agent, srcAgent *Agent) AgentDistanceType {
	if dstAgent == nil || srcAgent == nil {
		log.Logger.Error("nil dstAgent or nil srcAgent")
		return AGENT_DISTANCE_UNKNOWN
	}

	switch {
	case dstAgent.Idc == srcAgent.Idc:
		return AGENT_DISTANCE_SAMEIDC
	case dstAgent.Idc.Region == srcAgent.Idc.Region:
		return AGENT_DISTANCE_SAMEREGION
	default:
		return AGENT_DISTANCE_CROSSREGION
	}
}

func GetIdcPath(srcIdc *Idc, dstIdc *Idc) ([]*IdcLink, error) {
	// result to return
	links := make([]*IdcLink, 0)

	if srcIdc.Region == dstIdc.Region {
		// check if there is a direct link
		if link, exist := stateManager.networkManager.idcLinks[srcIdc.IdcName+"-"+dstIdc.IdcName]; exist {
			links = append(links, link)
			return links, nil
		}

		// no direct link, src->super core(named by region)->dst
		srcLink, exist := stateManager.networkManager.idcLinks[srcIdc.IdcName+"-"+srcIdc.Region.RegionName]
		if !exist {
			return nil, fmt.Errorf("no idc to supercore link found for idc %s", srcIdc.IdcName)
		}
		links = append(links, srcLink)

		dstLink, exist := stateManager.networkManager.idcLinks[dstIdc.Region.RegionName+"-"+dstIdc.IdcName]
		if !exist {
			return nil, fmt.Errorf("no supercore to idc link found for idc %s", dstIdc.IdcName)
		}
		links = append(links, dstLink)

		return links, nil
	} else {
		// src->src super core->dst super core->dst
		srcLink, exist := stateManager.networkManager.idcLinks[srcIdc.IdcName+"-"+srcIdc.Region.RegionName]
		if !exist {
			return nil, fmt.Errorf("no idc to supercore link found for idc %s", srcIdc.IdcName)
		}
		links = append(links, srcLink)

		supercoreLink, exist := stateManager.networkManager.idcLinks[srcIdc.Region.RegionName+"-"+dstIdc.Region.RegionName]
		if !exist {
			return nil, fmt.Errorf("no supercore to supercore link found for %s->%s",
				srcIdc.Region.RegionName, dstIdc.Region.RegionName)
		}
		links = append(links, supercoreLink)

		dstLink, exist := stateManager.networkManager.idcLinks[dstIdc.Region.RegionName+"-"+dstIdc.IdcName]
		if !exist {
			return nil, fmt.Errorf("no supercore to idc link found for idc %s", dstIdc.IdcName)
		}
		links = append(links, dstLink)

		return links, nil
	}
}

func GetLinkByName(linkname string) *IdcLink {
	link, _ := stateManager.networkManager.idcLinks[linkname]

	return link
}
