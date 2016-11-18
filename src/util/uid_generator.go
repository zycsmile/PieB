package util

import (
	"math/rand"
	"strconv"
	"time"
)

// Generate
func GenerateUid() (uid uint64) {
	// Time string
	const layout = "20060102030405"
	s := time.Now().Format(layout)

	// Time string in uint64
	timeUint64, err := strconv.ParseUint(s, 10, 64)
	for err != nil {
		s := time.Now().Format(layout)
		timeUint64, err = strconv.ParseUint(s, 10, 64)
	}

	// Generate a random appendix
	// Due to the length limitation of uint64, the appendix must be less than 100000
	appendix := rand.Intn(100000)

	return timeUint64*100000 + uint64(appendix)
}
