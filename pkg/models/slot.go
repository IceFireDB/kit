// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type SlotStatus string

const (
	SLOT_STATUS_ONLINE      SlotStatus = "online"
	SLOT_STATUS_OFFLINE     SlotStatus = "offline"
	SLOT_STATUS_MIGRATE     SlotStatus = "migrate"
	SLOT_STATUS_PRE_MIGRATE SlotStatus = "pre_migrate"
)

var (
	ErrSlotAlreadyExists = errors.New("slots already exists")
	ErrUnknownSlotStatus = errors.New("unknown slot status, slot status should be (online, offline, migrate, pre_migrate)")
)

type SlotMigrateStatus struct {
	From int `json:"from"`
	To   int `json:"to"`
}

type SlotMultiSetParam struct {
	From    int        `json:"from"`
	To      int        `json:"to"`
	Status  SlotStatus `json:"status"`
	GroupId int        `json:"group_id"`
}

type SlotState struct {
	Status        SlotStatus        `json:"status"`
	MigrateStatus SlotMigrateStatus `json:"migrate_status"`
	LastOpTs      string            `json:"last_op_ts"` // operation timestamp
}

type Slot struct {
	ProductName string    `json:"product_name"`
	Id          int       `json:"id"`
	GroupId     int       `json:"group_id"`
	State       SlotState `json:"state"`
}

func (s *Slot) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

func NewSlot(productName string, id int) *Slot {
	return &Slot{
		ProductName: productName,
		Id:          id,
		GroupId:     INVALID_ID,
		State: SlotState{
			Status:   SLOT_STATUS_OFFLINE,
			LastOpTs: "0",
			MigrateStatus: SlotMigrateStatus{
				From: INVALID_ID,
				To:   INVALID_ID,
			},
		},
	}
}

func (s *Slot) Encode() []byte {
	return jsonEncode(s)
}

func (s *Store) GetMigratingSlots() ([]Slot, error) {
	migrateSlots := make([]Slot, 0)
	slots, err := s.Slots()
	if err != nil {
		return nil, err
	}

	for _, slot := range slots {
		if slot.State.Status == SLOT_STATUS_MIGRATE {
			migrateSlots = append(migrateSlots, slot)
		}
	}

	return migrateSlots, nil
}

func (s *Store) Slots() ([]Slot, error) {
	slotPath := SlotDir(s.product)
	children, err := s.client.List(slotPath, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var slots []Slot
	for _, p := range children {
		data, err := s.client.Read(p, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		slot := Slot{}
		if err := json.Unmarshal(data, &slot); err != nil {
			return nil, errors.Trace(err)
		}
		slots = append(slots, slot)
	}

	return slots, nil
}

func (s *Store) SetMigrateStatus(slot *Slot, fromGroup, toGroup int) error {
	if fromGroup < 0 || toGroup < 0 {
		return errors.Errorf("invalid group id, from %d, to %d", fromGroup, toGroup)
	}
	// wait until all proxy confirmed
	err := s.NewAction(ACTION_TYPE_SLOT_PREMIGRATE, slot, "", true)
	if err != nil {
		return errors.Trace(err)
	}

	slot.State.Status = SLOT_STATUS_MIGRATE
	slot.State.MigrateStatus.From = fromGroup
	slot.State.MigrateStatus.To = toGroup

	slot.GroupId = toGroup

	return s.UpdateSlot(slot)
}
