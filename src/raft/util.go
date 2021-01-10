package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func ASSERT_EQUAL(left int, right int) {
	if (left != right) {
		log.Fatalf("%d and %d should be equal", left, right)
	}
}

func MIN(left int, right int) (int)  {
	if (left < right)  {
		return left
	} else{
		return right
	}

}

func MAX(left int, right int) (int)  {
	if (left > right)  {
		return left
	} else{
		return right
	}

}

func INITIALIZE(size int, default_value int) ([]int) {
	result := make([]int, size)
	for i := 0; i < size; i++ {
		result[i] = default_value
	}
	return result
}


