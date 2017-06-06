package main

import (
	"fmt"
	"github.com/immesys/spawnpoint/spawnable"
	"github.com/satori/go.uuid"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	// "sync"
	// "reflect"
	// "time"
)

const (
)

type Arbiter struct {
	bwClient *bw2.BW2Client
	baseuri string
	zcMap map[uuid.UUID]*ZoneController
}

func newArbiter(bwClient *bw2.BW2Client, baseuri string) *Arbiter {
	return &Arbiter {
		bwClient: bwClient,
		baseuri: baseuri,
		zcMap: make(map[uuid.UUID]*ZoneController),
	}
}

func (a *Arbiter) addZoneController(zc *ZoneController) {
	a.zcMap[zc.id] = zc
}

func (a *Arbiter) removeZoneController(zc *ZoneController) {
	delete(a.zcMap, zc.id)
}

func (a *Arbiter) run() {
	for _, zc := range a.zcMap {
		go func() {
			zc.run()
		}()
	}
	fmt.Println("All zone controllers running...")
	c := make(chan bool)
	<- c
}

// TODO: Standardize slot format
func (a *Arbiter) subscribeUpdatesSlot() {
	service := a.bwClient.RegisterService(a.baseuri, "s.vthermostat")
	iface := service.RegisterInterface("vthermostat", "i.xbos.thermostat")

	iface.SubscribeSlot("updates", func(msg *bw2.SimpleMessage) {
		po := msg.GetOnePODF(PONUM) // TODO: need new PONUM
		if po == nil {
			fmt.Println("Received actuation command without valid PO, dropping")
			return
		}

		msgpo, err := bw2.LoadMsgPackPayloadObject(po.GetPONum(), po.GetContents())
		if err != nil {
			fmt.Println(err)
			return
		}

		var data map[string]interface{}
		err = msgpo.ValueInto(&data)
		if err != nil {
			fmt.Println(err)
			return
		}

		//TODO: arbiter actuation
	})
}

func main() {
	bwClient := bw2.ConnectOrExit("")

	params := spawnable.GetParamsOrExit()
	bwClient.OverrideAutoChainTo(true)
	bwClient.SetEntityFromEnvironOrExit()

	baseuri := params.MustString("svc_base_uri")

	service := bwClient.RegisterService(baseuri, "s.arbiter")
	iface := service.RegisterInterface("arbiter", "i.xbos.thermostat")

	params.MergeMetadata(bwClient)

	sched1 := newSchedulerElem("scratch.ns/services/s.schedule/schedule/i.xbos.thermostat/signal/info",
		"scratch.ns/services/s.schedule/schedule/i.xbos.thermostat/slot/state",
		1)

	//inferior scheduler
	sched2 := newSchedulerElem("scratch.ns/services/s.schedule/schedule2/i.xbos.thermostat/signal/info",
		"scratch.ns/services/s.schedule/schedule2/i.xbos.thermostat/slot/state",
		0)

	schedulers := []*SchedulerElem{sched1, sched2}

	tstat := newTstatElem("scratch.ns/services/s.vthermostat/vthermostat/i.xbos.thermostat/signal/info",
		"scratch.ns/services/s.vthermostat/vthermostat/i.xbos.thermostat/slot/state")

	zc := newZoneController(bwClient, iface, schedulers, tstat, nil)
	
	a := newArbiter(bwClient, baseuri)
	a.addZoneController(zc)
	a.run()
}