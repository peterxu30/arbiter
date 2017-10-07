package main

import (
    //   "fmt"
    "github.com/satori/go.uuid"
    "sync"
)

// ScheduleMetadata stores metadata about the schedule and the scheduler that generated it.
// This data will be used to determine the generated schedule.
type ScheduleMetadata struct {
    schedulerId uuid.UUID
    priority int
}

func NewScheduleMetadata(schedulerId uuid.UUID, priority int) *ScheduleMetadata {
    return &ScheduleMetadata {
        schedulerId: schedulerId,
        priority: int
    }
}

// ScheduleData is the value type stored in the underlying map of the ScheduleFormulator.
type ScheduleData struct {
    schedule map[string]interface{}
    metadata *ScheduleMetadata
}

func NewScheduleData(schedulerId uuid.UUID, priority int, schedule map[string]interface{}) *ScheduleData {
    scheduleMetadata := NewScheduleMetadata(priority, schedulerId)
    return &ScheduleData {
        schedule: schedule,
        metadata: scheduleMetadata
    }
}

type ScheduleDetermination interface {
    AddSchedule(schedulerId uuid.UUID, scheduleData *ScheduleData)
    RemoveSchedule(schedulerId uuid.UUID)
    GenerateSchedule() map[string]interface{}
}

// ScheduleFormulator keeps track of the latest schedules the schedulers for a given zone have published.
// It will generate a master schedule that is a composite of the inputted schedules depending on user-specified determination scheme.
type ScheduleFormulator struct {
    scheduleMap sync.Map
}

func NewScheduleFormulator() *ScheduleFormulator {
    return &ScheduleFormulator {}
}

func (formulator *ScheduleFormulator) AddSchedule(schedulerId uuid.UUID, priority int, schedule map[string]interface{}) {
    scheduleData := NewScheduleData(schedulerId, priority, schedule)
    formulator.scheduleMap.Store(schedulerId, scheduleData)
}
