package main

import (
	// "fmt"
	// "github.com/immesys/spawnpoint/spawnable"
	// bw2 "gopkg.in/immesys/bw2bind.v5"
	// "sync"
	// "reflect"
	// "time"
)

const (
	// PONUM = "2.1.1.0"
	// DELAY = 60
)

type Arbiter struct {
	rootZC *ZoneController
}

func newArbiter(rootZC *ZoneController) *Arbiter {
	return &Arbiter {
		rootZC: rootZC,
	}
}