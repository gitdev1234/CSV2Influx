package csv

import (
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
