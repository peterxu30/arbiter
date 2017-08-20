package main

import (
	"fmt"
	"github.com/satori/go.uuid"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"sync"
	"reflect"
	"time"
)

const (
	PONUM = "2.1.1.0"
	DELAY = 60
)


// NewPO creates a new PayloadObject for the given message that conforms to the specified ponum.
func NewPO(msg map[string]interface{}, ponum string) (bw2.PayloadObject, error) {
	po, err := bw2.CreateMsgPackPayloadObject(bw2.FromDotForm(ponum), msg)
	if err != nil {
		return nil, err
	}
	return po, nil
}


// SchedulerElem contains the necessary information to publish and subscribe to a scheduler.
type SchedulerElem struct {
	SignalUri string
	SlotUri string
	Priority int
	SubscribeHandle string
	LastReceivedSchedule map[string]interface{}
}

// TstatElem contains the necessary information to publish and subscribe to a thermostat.
type TstatElem struct {
	SignalUri string
	SlotUri string
	SubscribeHandle string
	LastSentSchedule map[string]interface{}
	LastSentScheduleLock *sync.Mutex
}

// NewSchedulerElem creates a new SchedulerElem with the given signalUri, slotUri, and priority.
func NewSchedulerElem(signalUri string, slotUri string, priority int) *SchedulerElem {
	return &SchedulerElem {
		SignalUri: signalUri,
		SlotUri: slotUri,
		Priority: priority,
		SubscribeHandle: "",
		LastReceivedSchedule: nil,
	}
}

// NewTstatElem creates a new TstatElem with the given signalUri and slotUri.
func NewTstatElem(signalUri string, slotUri string) *TstatElem {
	return &TstatElem {
		SignalUri: signalUri,
		SlotUri: slotUri,
		SubscribeHandle: "",
		LastSentSchedule: nil,
		LastSentScheduleLock: &sync.Mutex{},
	}
}

// ZoneController arbitrates communication between schedulers and the thermostat of a given zone.
type ZoneController struct {
	id uuid.UUID
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

// NewZoneController creates a new ZoneController with SchedulerElems and TstatElem.
// The ZoneController should have reference to its containing ZoneController.
func NewZoneController(bwClient *bw2.BW2Client,
					   iface *bw2.Interface,
					   schedulers []*SchedulerElem, 
					   tstat *TstatElem, 
					   containingZC *ZoneController) *ZoneController {
	maxPriority := 0
	if len(schedulers) > 0 {
		maxPriority = schedulers[0].Priority
	}
	
	return &ZoneController {
		id: generateUUID(tstat.SignalUri),
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

func generateUUID(tstatSignalUri string) uuid.UUID {
	return uuid.NewV5(uuid.FromStringOrNil(tstatSignalUri), tstatSignalUri)
}

// AddScheduler creates and adds a new SchedulerElem with the given 
// signalUri, slotUri, and priority to the ZoneController, sorted by priority.
// AddScheduler is a wrapper around AddSchedulerElem.
func (zc *ZoneController) AddScheduler(signalUri string, slotUri string, priority int) {
	elem := NewSchedulerElem(signalUri, slotUri, priority)
	zc.AddSchedulerElem(elem)
}

// AddSchedulerElem adds the SchedulerElem to the ZoneController, sorted by priority.
func (zc *ZoneController) AddSchedulerElem(elem *SchedulerElem) {
	zc.schedulersLock.Lock()
	defer zc.schedulersLock.Unlock()

	for i, scheduler := range zc.schedulers {
		if scheduler.Priority < elem.Priority {
			zc.schedulers = append(append(zc.schedulers[:i], elem), zc.schedulers[i:]...)
			zc.SubscribeToScheduler(elem)
			return
		}
	}
}

// RemoveScheduler removes a SchedulerElem from the ZoneController that matches the given signalUri and slotUri.
// RemoveScheduler is a wrapper around RemoveSchedulerElem.
func (zc *ZoneController) RemoveScheduler(signalUri string, slotUri string) {
	elem := NewSchedulerElem(signalUri, slotUri, 0)
	zc.RemoveSchedulerElem(elem)
}

// RemoveSchedulerElem removes a SchedulerElem from the ZoneController
// that matches the given elem's signalUri and slotUri.
func (zc *ZoneController) RemoveSchedulerElem(elem *SchedulerElem) {
	zc.schedulersLock.Lock()
	defer zc.schedulersLock.Unlock()

	for i, scheduler := range zc.schedulers {
		if scheduler.SignalUri == elem.SignalUri && scheduler.SlotUri == elem.SlotUri {
			zc.schedulers = append(zc.schedulers[:i], zc.schedulers[i+1:]...)
			zc.bwClient.Unsubscribe(scheduler.SubscribeHandle)
			return
		}
	}
}

// UpdateScheduler updates a SchedulerElem's priority. The SchedulerElem's SignalUri and SlotUri must match the given arguments.
// UpdateScheduler is a wrapper around UpdateSchedulerElem.
func (zc *ZoneController) UpdateScheduler(signalUri string, slotUri string, priority int) {
	elem := NewSchedulerElem(signalUri, slotUri, priority)
	zc.UpdateSchedulerElem(elem)
}

// UpdateSchedulerElem updates a SchedulerElem's priority. The SchedulerElem's SignalUri and SlotUri must match the given elem's.
func (zc *ZoneController) UpdateSchedulerElem(elem *SchedulerElem) {
	zc.schedulersLock.Lock()
	defer zc.schedulersLock.Unlock()

	for _, scheduler := range zc.schedulers {
		if scheduler.SignalUri == elem.SignalUri && scheduler.SlotUri == elem.SlotUri {
			scheduler.Priority = elem.Priority
			return
		}
	}
}

// SetTstat sets and subsribes to a new thermostat as the thermostat of the ZoneController.
// The new thermostat has the given signalUri and slotUri.
// SetTstat is a wrapper around SetTstatElem.
func (zc *ZoneController) SetTstat(signalUri string, slotUri string) {
	elem := NewTstatElem(signalUri, slotUri)
	zc.SetTstatElem(elem)
}

// SetTstat sets and subsribes to a new thermostat as the thermostat of the ZoneController.
func (zc *ZoneController) SetTstatElem(elem *TstatElem) {
	zc.tstat = elem
	zc.SubscribeToTstat()
}

// RemoveTstat removes and unsubscribes from the thermostat of the ZoneController.
// This will leave the ZoneController without a thermostat.
func (zc *ZoneController) RemoveTstat() {
	zc.bwClient.Unsubscribe(zc.tstat.SubscribeHandle)
	zc.tstat = nil
}

// Run initiates operation of the ZoneController. It will subscribe to all schedulers and the thermostat
// and mediate communication between the them.
func (zc *ZoneController) Run() {
	zc.SubscribeToAllSchedulers()
	zc.SubscribeToTstat()
	zc.PublishToSchedulers()
	zc.PublishToThermostat()
	running := make(chan bool)
	<- running
}

// SubscribeToAllSchedulers subscribes the ZoneController to all schedulers.
func (zc *ZoneController) SubscribeToAllSchedulers() {
	zc.schedulersLock.Lock()
	defer zc.schedulersLock.Unlock()

	for _, scheduler := range zc.schedulers {
		zc.SubscribeToScheduler(scheduler)
	}
}

// SubscribeToScheduler subscribes the ZoneController to the given scheduler.
func (zc *ZoneController) SubscribeToScheduler(scheduler *SchedulerElem) error {
	signalParams := &bw2.SubscribeParams {
		URI: scheduler.SignalUri,
	}
	subscribeScheduleChan, handle, err := zc.bwClient.SubscribeH(signalParams)

	if err != nil {
		return err
	}

	scheduler.SubscribeHandle = handle

	go func() {
		fmt.Println("Beginning subscribe to scheduler")
		for msg := range subscribeScheduleChan {
			fmt.Println("recvd sched msg")
			po := msg.GetOnePODF(PONUM)
			if po == nil {
				fmt.Println("Received actuation command without valid PO, dropping")
				continue
			}

			msgpo, err := bw2.LoadMsgPackPayloadObject(po.GetPONum(), po.GetContents())
			if err != nil {
				fmt.Println(err)
				continue
			}

			var schedule map[string]interface{}
			err = msgpo.ValueInto(&schedule)
			fmt.Println(schedule)
			if err != nil {
				fmt.Println(err)
				continue
			}

			if zc.isValidSchedule(scheduler, schedule) { //TODO: need to synchronize this with locks for changing priorities
				zc.scheduleChan <- schedule
			}
			scheduler.LastReceivedSchedule = schedule
		}
	}()

	return nil
}

// Logic that determines if a schedule is valid goes in here. (e.g. Max/min cooling setpoint, max/min heating setpoint, )
func (zc *ZoneController) isValidSchedule(scheduler *SchedulerElem, schedule map[string]interface{}) bool {
	return zc.maxPriority == scheduler.Priority && !reflect.DeepEqual(scheduler.LastReceivedSchedule, schedule)
}

func (zc *ZoneController) SubscribeToTstat() {
	zc.subscribeToTstat(zc.tstat)
}

func (zc *ZoneController) subscribeToTstat(tstat *TstatElem) {
	signalParams := &bw2.SubscribeParams {
		URI: tstat.SignalUri,
	}
	subscribeTstatChan, handle, err := zc.bwClient.SubscribeH(signalParams)

	if err != nil {
		panic(err)
	}

	tstat.SubscribeHandle = handle
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

			tstat.LastSentScheduleLock.Lock()
			if (tstatData["time"].(uint64) + DELAY < uint64(time.Now().UnixNano())) || (tstatData["override"] == true ||
				(tstatData["heating_setpoint"] == zc.tstat.LastSentSchedule["heating_setpoint"] &&
				tstatData["cooling_setpoint"] == zc.tstat.LastSentSchedule["cooling_setpoint"] &&
				tstatData["mode"] == zc.tstat.LastSentSchedule["mode"])) {
				//last sent schedule was delivered.
				fmt.Println(tstatData)
				zc.tstatDataChan <- tstatData
			} else {
				fmt.Println("msg not delivered")
				zc.scheduleChan <- zc.tstat.LastSentSchedule
			}
			tstat.LastSentScheduleLock.Unlock()
		}
	}()
}

func (zc *ZoneController) PublishToThermostat() {
	go func() {
		for schedule := range zc.scheduleChan {
			zc.tstat.LastSentScheduleLock.Lock()
			if zc.tstat.LastSentSchedule != nil && schedule["time"].(uint64) < zc.tstat.LastSentSchedule["time"].(uint64) {
				zc.tstat.LastSentScheduleLock.Unlock()
				continue
			}
			zc.tstat.LastSentScheduleLock.Unlock()

			schedulePO, err := NewPO(schedule, PONUM)
			if err != nil {
				fmt.Println(err)
				continue
			}

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

func (zc *ZoneController) PublishToSchedulers() {
	go func() {
		for tstatData := range zc.tstatDataChan {
			fmt.Println("Publishing tstat data")
			tstatDataPO, err := NewPO(tstatData, PONUM)
			if err != nil {
				fmt.Println(err)
				continue
			}

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
