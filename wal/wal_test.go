package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestWAL_something(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	fmt.Println(dir)
	defer os.RemoveAll(dir)

	wal, err := Open(dir, 0, nil, nil)
	if err != nil {
		panic(err)
	}
	err = wal.ReadAll(func([]byte) error {
		return nil
	})
	if err != nil {
		panic(err)
	}

	rec := make([]byte, 2000)

	for i := 0; i < 16; i++ {
		go func() {
			for {
				err := wal.Log(rec)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				// time.Sleep(time.Microsecond * 100)
			}
		}()
	}
	time.Sleep(10 * time.Second)
}
