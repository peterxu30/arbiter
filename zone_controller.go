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

type BWElem interface {
	GetSignalUri() string
	SetSignalUri(signalUri string)
	GetSlotUri() string
	SetSlotUri(slotUri string)
}

// SchedulerElem contains the necessary information to publish and subscribe to a scheduler.
type SchedulerElem struct {
	SignalUri string
	SlotUri string
	Priority int
	SubscribeHandle string
	LastReceivedSchedule map[string]interface{}
}

func (scheduler *SchedulerElem) GetSignalUri() string {
	return scheduler.SignalUri
}

func (scheduler *SchedulerElem) SetSignalUri(signalUri string) {
	scheduler.SignalUri = signalUri
}

func (scheduler *SchedulerElem) GetSlotUri() string {
	return scheduler.SlotUri
}

func (scheduler *SchedulerElem) SetSlotUri(slotUri string) {
	scheduler.SlotUri = slotUri
}

// TstatElem contains the necessary information to publish and subscribe to a thermostat.
type TstatElem struct {
	SignalUri string
	SlotUri string
	SubscribeHandle string
	LastSentSchedule map[string]interface{}
	LastSentScheduleLock *sync.Mutex
}

func (tstat *TstatElem) GetSignalUri() string {
	return tstat.SignalUri
}

func (tstat *TstatElem) SetSignalUri(signalUri string) {
	tstat.SignalUri = signalUri
}

func (tstat *TstatElem) GetSlotUri() string {
	return tstat.SlotUri
}

func (tstat *TstatElem) SetSlotUri(slotUri string) {
	tstat.SlotUri = slotUri
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
	schedulers sync.Map
	tstats sync.Map
	scheduleChan chan map[string]interface{}
	tstatDataChan chan map[string]interface{}
	containingZC *ZoneController
}

// NewZoneController creates a new ZoneController with SchedulerElems and TstatElem.
// The ZoneController should have reference to its containing ZoneController.
func NewZoneController(bwClient *bw2.BW2Client,
					   iface *bw2.Interface,
					   schedulers []*SchedulerElem, 
					   tstats []*TstatElem, 
					   containingZC *ZoneController) *ZoneController {
	maxPriority := 0
	if len(schedulers) > 0 {
		maxPriority = schedulers[0].Priority
	}

	var schedulerMap sync.Map
	var tstatMap sync.Map

	for _, scheduler := range schedulers {
		schedulerId := generateBWElemUUID(scheduler)
		schedulerMap.Store(schedulerId, scheduler)
	}

	for _, tstat := range tstats {
		tstatId := generateBWElemUUID(tstat)
		tstatMap.Store(tstatId, tstat)
	}
	
	return &ZoneController {
		id: generateRandomUUID(),
		bwClient: bwClient,
		iface: iface,
		maxPriority: maxPriority,
		schedulers: schedulerMap,
		tstats: tstatMap,
		scheduleChan: make(chan map[string]interface{}, 5),
		tstatDataChan: make(chan map[string]interface{}, 5),
		containingZC: containingZC,
	}
}

func generateBWElemUUID(elem BWElem) uuid.UUID {
	return generateUUID(elem.GetSignalUri(), elem.GetSlotUri())
}

func generateUUID(ns string, name string) uuid.UUID {
	return uuid.NewV5(uuid.FromStringOrNil(ns), name)
}

func generateRandomUUID() uuid.UUID {
	return uuid.NewV4()
}

// AddScheduler creates and adds a new SchedulerElem with the given 
// signalUri, slotUri, and priority to the ZoneController, sorted by priority.
// If a scheduler exists with the given signalUri and slotUri, it will be replaced.
// AddScheduler is a wrapper around AddSchedulerElem.
func (zc *ZoneController) AddOrUpdateScheduler(signalUri string, slotUri string, priority int) {
	elem := NewSchedulerElem(signalUri, slotUri, priority)
	zc.AddOrUpdateSchedulerElem(elem)
}

// AddSchedulerElem adds the SchedulerElem to the ZoneController, sorted by priority.
// If a scheduler exists with the given signalUri and slotUri, it will be replaced.
func (zc *ZoneController) AddOrUpdateSchedulerElem(elem *SchedulerElem) {
	schedulerId := generateBWElemUUID(elem)
	zc.schedulers.Store(schedulerId, elem)
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
	schedulerId := generateBWElemUUID(elem)
	zc.schedulers.Delete(schedulerId)
}

// SetTstat sets and subsribes to a new thermostat as the thermostat of the ZoneController.
// The new thermostat has the given signalUri and slotUri.
// SetTstat is a wrapper around SetTstatElem.
func (zc *ZoneController) AddOrUpdateTstat(signalUri string, slotUri string) {
	elem := NewTstatElem(signalUri, slotUri)
	zc.AddOrUpdateTstatElem(elem)
}

// SetTstat sets and subsribes to a new thermostat as the thermostat of the ZoneController.
func (zc *ZoneController) AddOrUpdateTstatElem(elem *TstatElem) {
	tstatId := generateBWElemUUID(elem)
	oldTstatInterface, ok := zc.tstats.Load(tstatId)
	if ok {
		oldTstat, ok := oldTstatInterface.(*TstatElem)
		if !ok {
			fmt.Println("Cast failed")
			return
		}
		zc.bwClient.Unsubscribe(oldTstat.SubscribeHandle)
	}
	zc.tstats.Store(tstatId, elem)
	zc.subscribeToTstat(elem)
}

// RemoveTstat removes and unsubscribes from the thermostat of the ZoneController.
// This will leave the ZoneController without a thermostat.
func (zc *ZoneController) RemoveTstat(signalUri string, slotUri string) {
	elem := NewTstatElem(signalUri, slotUri)
	zc.RemoveTstatElem(elem)
}

func (zc *ZoneController) RemoveTstatElem(tstat *TstatElem) {
	tstatId := generateBWElemUUID(tstat)
	zc.bwClient.Unsubscribe(tstat.SubscribeHandle)
	zc.tstats.Delete(tstatId)
}

// Run initiates operation of the ZoneController. It will subscribe to all schedulers and the thermostat
// and mediate communication between the them.
func (zc *ZoneController) Run() {
	zc.SubscribeToAllSchedulers()
	zc.SubscribeToAllTstats()
	zc.PublishToAllSchedulers()
	zc.PublishToAllThermostats()
	running := make(chan bool)
	<- running
}

// SubscribeToAllSchedulers subscribes the ZoneController to all schedulers.
func (zc *ZoneController) SubscribeToAllSchedulers() {
	zc.schedulers.Range(func(key, value interface{}) bool {
		err := zc.subscribeToScheduler(value.(*SchedulerElem))
		if err != nil {
			fmt.Println(err)
		}
		return true
	})
}

// SubscribeToScheduler subscribes the ZoneController to the given scheduler.
func (zc *ZoneController) subscribeToScheduler(scheduler *SchedulerElem) error {
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

func (zc *ZoneController) SubscribeToAllTstats() {
	zc.tstats.Range(func(key, value interface{}) bool {
		zc.subscribeToTstat(value.(*TstatElem))
		return true
	})
}

//TODO: Fix
func (zc *ZoneController) subscribeToTstat(tstat *TstatElem) {
	signalParams := &bw2.SubscribeParams {
		URI: tstat.SignalUri,
	}
	subscribeTstatChan, handle, err := zc.bwClient.SubscribeH(signalParams)

	if err != nil {
		panic(err)
	}

	tstat.SubscribeHandle = handle
	fmt.Println("Subscribing to tstat", tstat.SignalUri)
	
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
				(tstatData["heating_setpoint"] == tstat.LastSentSchedule["heating_setpoint"] &&
				tstatData["cooling_setpoint"] == tstat.LastSentSchedule["cooling_setpoint"] &&
				tstatData["mode"] == tstat.LastSentSchedule["mode"])) {
				//last sent schedule was delivered.
				fmt.Println(tstatData)
				zc.tstatDataChan <- tstatData
			} else {
				fmt.Println("msg not delivered")
				zc.scheduleChan <- tstat.LastSentSchedule
			}
			tstat.LastSentScheduleLock.Unlock()
		}
	}()
}

//TODO: Fix
func (zc *ZoneController) PublishToAllThermostats() {
	// go func() {
	// 	for schedule := range zc.scheduleChan {
	// 		zc.tstat.LastSentScheduleLock.Lock()
	// 		if zc.tstat.LastSentSchedule != nil && schedule["time"].(uint64) < zc.tstat.LastSentSchedule["time"].(uint64) {
	// 			zc.tstat.LastSentScheduleLock.Unlock()
	// 			continue
	// 		}
	// 		zc.tstat.LastSentScheduleLock.Unlock()

	// 		schedulePO, err := NewPO(schedule, PONUM)
	// 		if err != nil {
	// 			fmt.Println(err)
	// 			continue
	// 		}

	// 		zc.schedulersLock.Lock()
	// 		zc.publishPO(zc.tstat.SlotUri, schedulePO)
	// 		fmt.Println("Published schedule", schedule, zc.tstat.SlotUri)

	// 		zc.tstat.LastSentScheduleLock.Lock()
	// 		zc.tstat.LastSentSchedule = schedule
	// 		zc.tstat.LastSentScheduleLock.Unlock()

	// 		zc.schedulersLock.Unlock()
	// 	}
	// }()
}

//TODO: Fix
func (zc *ZoneController) PublishToAllSchedulers() {
	// go func() {
	// 	for tstatData := range zc.tstatDataChan {
	// 		fmt.Println("Publishing tstat data")
	// 		tstatDataPO, err := NewPO(tstatData, PONUM)
	// 		if err != nil {
	// 			fmt.Println(err)
	// 			continue
	// 		}

	// 		zc.schedulersLock.Lock()
	// 		for _, scheduler := range zc.schedulers {
	// 			zc.publishPO(scheduler.SlotUri, tstatDataPO)
	// 			fmt.Println("Published thermostat data", tstatData, scheduler.SlotUri)
	// 		}
	// 		zc.schedulersLock.Unlock()
	// 	}
	// }()
}

func (zc *ZoneController) publishPO(uri string, po bw2.PayloadObject) {
	publishParams := &bw2.PublishParams {
		URI: uri,
		PayloadObjects: []bw2.PayloadObject{po},
	}
	zc.bwClient.Publish(publishParams)
}
