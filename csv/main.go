package csv

import (
	"strconv"
	"time"
)

type Line struct {
	Tag   string
	Time  time.Time
	Value float64
}

type MergedLines struct {
	LinesTime map[time.Time]map[string]interface{}
}

func (m *MergedLines) Add(record []string) {
	timestamp, _ := time.Parse("2006-01-02 15:04:05Z", record[1])
	value, _ := strconv.ParseFloat(record[2], 64)
	field := record[0]

	if m.LinesTime == nil {
		m.LinesTime = make(map[time.Time]map[string]interface{})
	}
	line := m.LinesTime[timestamp]
	if line == nil {
		line = make(map[string]interface{})
	}
	line[field] = value
	m.LinesTime[timestamp] = line
}
