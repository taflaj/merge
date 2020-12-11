// models/models.go

package models

import (
	"strings"
	"time"
)

// Record contains the formatted message shared on the pubsub channel
type Record struct {
	// TimeStamp
	TimeStamp *time.Time
	// Name
	Name string
	// Node
	Node string
	// Message
	Message string
}

// Layout specifies how dates are rendered
const Layout = "20060102 150405 MST"

func getTime(when string) (*time.Time, error) {
	// convert string to internal format
	s := when[:8] + " " + when[13:19] + " " + when[20:]
	// log.Printf(s)
	t, err := time.Parse(Layout, s)
	if err != nil {
		return nil, err
	}
	// adjust to the local time zone
	local := t.Local()
	// log.Printf("input: %v; time: %v; local: %v; unix: %v/%v", s, t, local, t.Unix(), local.Unix())
	return &local, nil
}

// NewRecord creates a new record based on the input text
// Input is formatted as YYYYMMDD-ddd-HHMMSS-ZZZ|Name_ID|Optional text
func NewRecord(input string) (*Record, error) {
	// break down in major components
	// log.Println(input)
	splits := strings.Split(input, "|")
	// log.Printf("Splits: %#v\n", splits)
	if len(splits) < 3 {
		return nil, nil
	}
	// get time stamp
	when := splits[0]
	if len(when) != 23 {
		return nil, nil
	}
	// stamp := splits[0][:8] + " " + splits[0][13:19]
	stamp, err := getTime(when)
	if err != nil {
		return nil, err
	}
	// get name and id
	who := splits[1]
	p := strings.Index(who, "_")
	if p == -1 {
		return nil, nil
	}
	name := who[:p]
	id := who[p+1:]
	// get message
	p = len(when) + len(who) + 2
	message := input[p:]
	// log.Printf("Stamp: %v; Name: %v; ID: %v\n  %v\n", stamp, name, id, message)
	return &Record{stamp, name, id, message}, nil
}
