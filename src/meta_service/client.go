/* client.go - client for meta_service */
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package meta_service

import (
	"math"
    "strconv"
    "strings"
//    "fmt"
	//"math/rand"
)
import (
//    "www.baidu.com/golang-lib/log"
)
import (
	. "types"
//	"util"
)

var (
	Simulation = true
)

// TODO: this is a simuliated interface
func GetDataMeta(url string) (*Data, error) {
	if Simulation {
		d := &Data{
//			DataId:          util.GenerateUid(), //uint64(1111),
			OriginPath:      url,
//			OriginAgentName: "test.yf.baidu.com",
			Size:            int64(20 * 1024), //27G-->10 blks
			BlockSize:       int64(2 * 1024),      // 2MB in KB
		}
//        pathlen := len(url)
        d.OriginAgentName = url[7:]

		// calc block count
		d.BlockCount = int(math.Ceil(float64(d.Size) / float64(d.BlockSize)))
//        log.Logger.Info("BlockCount:%d",d.BlockCount)

		// add blocks
        
        slice := strings.Split(url,".")
        need := slice[0]
        order,_ := strconv.Atoi(need[21:])

        d.DataId = uint64(order*1000)
//        log.Logger.Info("here getDataMeta:%d",d.DataId)
		d.Blocks = make([]*Block, d.BlockCount)
		for i := 0; i < d.BlockCount; i++ {
//            blkNum := d.BlockCount * (order - 1) + i
			blk := &Block{
				Data:       d,
				BlockIndex: i,
				Md5:        uint32(0),
			}

			// set block size
			if i < d.BlockCount-1 {
				blk.Size = d.BlockSize
			} else {
				blk.Size = d.Size - int64(d.BlockCount-1)*d.BlockSize
			}

			d.Blocks[i] = blk
		}

		return d, nil
	}

	return nil, nil
}
