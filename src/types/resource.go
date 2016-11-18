/* resource.go - the definition and interface for resource */
/*
modification history
--------------------
2015/4/22, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package types

type Resource struct {
    ResourceId uint64

    Capability int64 // total bw capability, in KB/s
    Free       int64 // free bw resource, in KB/s
}
