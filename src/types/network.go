/* network.go - definition of network elements  */
/*
modification history
--------------------
2015/4/21, by Guang Yao, create
*/
/*
DESCRIPTION
*/

package types

type IdcStatus int

const (
	IDC_STATUS_LIVE IdcStatus = iota
	IDC_STATUS_DEAD
)

type IdcLinkStatus int

const (
	IDC_LINK_STATUS_UP IdcLinkStatus = iota
	IDC_LINK_STATUS_DOWN
)

// IDC
type Idc struct {
	IdcName string // idc name

	Region *Region // region

	Status IdcStatus // live or dead
}

// IDC link
type IdcLink struct {
	SrcIdc *Idc
	DstIdc *Idc

	LinkName string // each IDC link has a name; e.g., link from yf to hb super core
	// is named "yf-hb"

	Status IdcLinkStatus // up or down

	Quota int64 // total bandwidth quota on the IDC link; in KB/s
	Free  int64 // current free
}

// region
type Region struct {
	RegionName string // name of the region

	Idcs map[string]*Idc // all the idc in the region
}

// region link
type RegionLink struct {
	SrcRegion *Region // source region
	DstRegion *Region // dst region

	LinkName string // "hb-hd" or "hd-hb"

	Quota int64 // total bandwidth quota on the IDC link; in KB/s

	ProductQuota map[string]int64 // quota of each product on this link; product name => quota
}
