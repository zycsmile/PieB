package util

import (
	"fmt"
	"testing"
	"time"
)

func TestGenerateUid(t *testing.T) {
	start := time.Now()
	for i := 0; i < 1000; i++ {
		GenerateUid()
	}
	elapse := time.Since(start)

	fmt.Printf("%s\n", elapse.String())
}
