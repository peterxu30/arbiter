package main

import (
	"fmt"
	"github.com/immesys/spawnpoint/spawnable"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"sync"
	"reflect"
	"time"
)

const (
	PONUM = "2.1.1.0"
	DELAY = 60
)

func NewPO(msg map[string]interface{}) (bw2.PayloadObject) { //Will this work for both thermostats and schedulers?
	po, err := bw2.CreateMsgPackPayloadObject(bw2.FromDotForm(PONUM), msg)
	if err != nil {
		panic(err)
	}
	return po
}

type SchedulerElem struct {
	SignalParams *bw2.SubscribeParams
	SignalUri string
	SlotUri string
	Priority int
	SubscribeHandle string
	LastReceivedSchedule map[string]interface{}
}

type TstatElem struct {
	SignalParams *bw2.SubscribeParams
	SignalUri string
	SlotUri string
	SubscribeHandle string
	LastSentSchedule map[string]interface{}
	LastSentScheduleLock *sync.Mutex
}

func newSchedulerElem(signalUri string, slotUri string, priority int) *SchedulerElem {
	signalParams := &bw2.SubscribeParams {
		URI: signalUri,
	}

	return &SchedulerElem {
		SignalParams: signalParams,
		SignalUri: signalUri,
		SlotUri: slotUri,
		Priority: priority,
		SubscribeHandle: "",
		LastReceivedSchedule: nil,
	}
}

func newTstatElem(signalUri string, slotUri string) *TstatElem {
	signalParams := &bw2.SubscribeParams {
		URI: signalUri,
	}

	return &TstatElem {
		SignalParams: signalParams,
		SignalUri: signalUri,
		SlotUri: slotUri,
		SubscribeHandle: "",
		LastSentSchedule: nil,
		LastSentScheduleLock: &sync.Mutex{},
	}
}

type ZoneController struct {
	bwClient *bw2.BW2Client
	iface *bw2.Interface
	maxPriority int
	schedulers []*SchedulerElem
	tstat *TstatElem
	schedulersLock *sync.Mutex
	scheduleChan chan map[string]interface{}
	tstatDataChan chan map[string]interface{}
	containingZC *ZoneController
}

func newZoneController(bwClient *bw2.BW2Client, iface *bw2.Interface, schedulers []*SchedulerElem, tstat *TstatElem, containingZC *ZoneController) *ZoneController {
	maxPriority := 0
	if len(schedulers) > 0 && schedulers[0] != nil {
		maxPriority = schedulers[0].Priority
	}
	
	return &ZoneController {
		bwClient: bwClient,
		iface: iface,
		maxPriority: maxPriority,
		schedulers: schedulers,
		tstat: tstat,
		schedulersLock: &sync.Mutex{},
		scheduleChan: make(chan map[string]interface{}, 5),
		tstatDataChan: make(chan map[string]interface{}, 5),
		containingZC: containingZC,
	}
}

func (zc *ZoneController) addScheduler(signalUri string, slotUri string, priority int) {
	zc.schedulersLock.Lock()
	elem := newSchedulerElem(signalUri, slotUri, priority)
	zc.addSchedulerElem(elem)
	zc.schedulersLock.Unlock()
}

func (zc *ZoneController) addSchedulerElem(elem *SchedulerElem) {
	for i, scheduler := range zc.schedulers {
		if scheduler.Priority < elem.Priority {
			zc.schedulers = append(append(zc.schedulers[:i], elem), zc.schedulers[i:]...)
			zc.subscribeToScheduler(elem)
			return
		}
	}
}
//TODO: Need to fix all remove methods because subscribe takes in a handle string. Makes more sense to write functions using elem and wrapper functions for strings.
func (zc *ZoneController) removeScheduler(signalUri string, slotUri string) {
	zc.schedulersLock.Lock()
	for i, scheduler := range zc.schedulers {
		if scheduler.SignalUri == signalUri && scheduler.SlotUri == slotUri {
			zc.schedulers = append(zc.schedulers[:i], zc.schedulers[i+1:]...)
			zc.bwClient.Unsubscribe(scheduler.SubscribeHandle)
			return
		}
	}
	zc.schedulersLock.Unlock()
}

// func (zc *ZoneController) removeSchedulerElem(elem *SchedulerElem) {
// 	zc.removeScheduler(elem.SignalUri, elem.SlotUri)
// }

func (zc *ZoneController) updateScheduler(signalUri string, slotUri string, priority int) {
	zc.removeScheduler(signalUri, slotUri)
	zc.addScheduler(signalUri, slotUri, priority)
}

func (zc *ZoneController) setTstat(signalUri string, slotUri string) {
	elem := newTstatElem(signalUri, slotUri)
	zc.setTstatElem(elem)
}

func (zc *ZoneController) setTstatElem(elem *TstatElem) {
	zc.tstat = elem
	zc.subscribeToThermostat()
}

func (zc *ZoneController) removeTstat(signalUri string, slotUri string) {
	if zc.tstat.SignalUri == signalUri && zc.tstat.SlotUri == slotUri {
		zc.bwClient.Unsubscribe(zc.tstat.SubscribeHandle)
		zc.tstat = nil
		return
	}
}

func (zc *ZoneController) removeTstatElem(elem *TstatElem) {
	zc.removeTstat(elem.SignalUri, elem.SlotUri)
}

func (zc *ZoneController) run() {
	zc.subscribeToAllSchedulers()
	zc.subscribeToThermostat()
	zc.publishToThermostat()
	zc.publishToSchedulers()
	c := make(chan bool)
	<- c
}

func (zc *ZoneController) subscribeToAllSchedulers() {
	zc.schedulersLock.Lock()
	for _, scheduler := range zc.schedulers {
		zc.subscribeToScheduler(scheduler)
	}
	zc.schedulersLock.Unlock()
}

func (zc *ZoneController) subscribeToScheduler(scheduler *SchedulerElem) {
	subscribeScheduleChan, handle, err := zc.bwClient.SubscribeH(scheduler.SignalParams)
	// fmt.Println(sp.URI, sp.PrimaryAccessChain, sp.AutoChain, sp.RoutingObjects, sp.Expiry, sp.ExpiryDelta, sp.ElaboratePAC, sp.DoNotVerify, sp.LeavePacked)

	if err != nil {
		panic(err)
	}

	scheduler.SubscribeHandle = handle
	fmt.Println("Subscribed to scheduler signal")
	// fmt.Println(scheduler.SignalParams)

	go func() {
		fmt.Println("Beginning subscribe to scheduler")
		for msg := range subscribeScheduleChan {
			fmt.Println("recvd sched msg")
			po := msg.GetOnePODF(PONUM)
			if po == nil {
				fmt.Println("Received actuation command without valid PO, dropping")
				return
			}

			msgpo, err := bw2.LoadMsgPackPayloadObject(po.GetPONum(), po.GetContents())
			if err != nil {
				fmt.Println(err)
				return
			}

			var schedule map[string]interface{}
			err = msgpo.ValueInto(&schedule)
			fmt.Println(schedule)
			if err != nil {
				fmt.Println(err)
				return
			}

			if zc.isValidSchedule(scheduler, schedule) { //TODO: need to synchronize this with locks for changing priorities
				zc.scheduleChan <- schedule
			}
			scheduler.LastReceivedSchedule = schedule
		}
	}()
}

// Logic that determines if a schedule is valid goes in here. (e.g. Max/min cooling setpoint, max/min heating setpoint, )
func (zc *ZoneController) isValidSchedule(scheduler *SchedulerElem, schedule map[string]interface{}) bool {
	return zc.maxPriority == scheduler.Priority && !reflect.DeepEqual(scheduler.LastReceivedSchedule, schedule)
}

func (zc *ZoneController) subscribeToThermostat() {
	subscribeTstatChan, handle, err := zc.bwClient.SubscribeH(zc.tstat.SignalParams)

	if err != nil {
		panic(err)
	}

	zc.tstat.SubscribeHandle = handle
	fmt.Println("Subscribing to tstat", zc.tstat.SignalUri)
	
	go func() {
		for msg := range subscribeTstatChan {
			fmt.Println("RECEIVED tstat msg")
			po := msg.GetOnePODF(PONUM)
			if po == nil {
				fmt.Println("Received actuation command without valid PO, dropping")
				return
			}

			msgpo, err := bw2.LoadMsgPackPayloadObject(po.GetPONum(), po.GetContents())
			if err != nil {
				fmt.Println(err)
				return
			}

			var tstatData map[string]interface{}
			err = msgpo.ValueInto(&tstatData)
			if err != nil {
				fmt.Println(err)
				return
			}

			zc.tstat.LastSentScheduleLock.Lock()
			if (tstatData["time"].(int64) + DELAY < time.Now().UnixNano()) ||
				(tstatData["override"] == true || (tstatData["heating_setpoint"] == zc.tstat.LastSentSchedule["heating_setpoint"] &&
				tstatData["cooling_setpoint"] == zc.tstat.LastSentSchedule["cooling_setpoint"] &&
				tstatData["mode"] == zc.tstat.LastSentSchedule["mode"])) {
				//last sent schedule was delivered.
				fmt.Println(tstatData)
				zc.tstatDataChan <- tstatData
			} else {
				fmt.Println("msg not delivered")
				zc.scheduleChan <- zc.tstat.LastSentSchedule
			}
			zc.tstat.LastSentScheduleLock.Unlock()
		}
	}()
}

func (zc *ZoneController) publishToThermostat() {
	go func() {
		for schedule := range zc.scheduleChan {
			zc.tstat.LastSentScheduleLock.Lock()
			if schedule["time"].(int64) < zc.tstat.LastSentSchedule["time"].(int64) {
				continue
			}
			zc.tstat.LastSentScheduleLock.Unlock()

			schedulePO := NewPO(schedule)
			zc.schedulersLock.Lock()
			zc.publishPO(zc.tstat.SlotUri, schedulePO)
			fmt.Println("Published schedule", schedule, zc.tstat.SlotUri)

			zc.tstat.LastSentScheduleLock.Lock()
			zc.tstat.LastSentSchedule = schedule
			zc.tstat.LastSentScheduleLock.Unlock()

			zc.schedulersLock.Unlock()
		}
	}()
}

func (zc *ZoneController) publishToSchedulers() {
	go func() {
		for tstatData := range zc.tstatDataChan {
			fmt.Println("Publishing tstat data")
			tstatDataPO := NewPO(tstatData)
			zc.schedulersLock.Lock()
			for _, scheduler := range zc.schedulers {
				zc.publishPO(scheduler.SlotUri, tstatDataPO)
				fmt.Println("Published thermostat data", tstatData, scheduler.SlotUri)
			}
			zc.schedulersLock.Unlock()
		}
	}()
}

func (zc *ZoneController) publishPO(uri string, po bw2.PayloadObject) {
	publishParams := &bw2.PublishParams {
		URI: uri,
		PayloadObjects: []bw2.PayloadObject{po},
	}
	zc.bwClient.Publish(publishParams)
}

func main() {
	bwClient := bw2.ConnectOrExit("")

	params := spawnable.GetParamsOrExit()
	bwClient.OverrideAutoChainTo(true)
	bwClient.SetEntityFromEnvironOrExit()

	baseuri := params.MustString("svc_base_uri")
	// poll_interval := params.MustString("poll_interval")

	service := bwClient.RegisterService(baseuri, "s.arbiter")
	iface := service.RegisterInterface("arbiter", "i.xbos.thermostat")

	params.MergeMetadata(bwClient)

	sched1 := newSchedulerElem("scratch.ns/services/s.schedule/schedule1/i.xbos.thermostat/signal/info", //TODO: CHANGE SCHEDULER URIs
		"scratch.ns/services/s.schedule/schedule1/i.xbos.thermostat/slot/state",
		1)

	//inferior scheduler
	sched2 := newSchedulerElem("scratch.ns/services/s.schedule/schedule2/i.xbos.thermostat/signal/info",
		"scratch.ns/services/s.schedule/schedule2/i.xbos.thermostat/slot/state",
		0)

	schedulers := []*SchedulerElem{sched1, sched2}

	tstat := newTstatElem("scratch.ns/services/s.vthermostat/vthermostat/i.xbos.thermostat/signal/info",
		"scratch.ns/services/s.vthermostat/vthermostat/i.xbos.thermostat/slot/state")

	zc := newZoneController(bwClient, iface, schedulers, tstat, nil)
	zc.run()
}
