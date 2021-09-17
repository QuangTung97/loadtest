package loadtest

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"
)

// RunResultList ...
type RunResultList []RunResult

var _ sort.Interface = RunResultList{}

func (l RunResultList) Len() int {
	return len(l)
}

func (l RunResultList) Less(i, j int) bool {
	return l[i].Duration < l[j].Duration
}

func (l RunResultList) Swap(i, j int) {
	l[j], l[i] = l[i], l[j]
}

// Sort ...
func (l RunResultList) Sort() {
	sort.Sort(l)
}

// SortByRequestID ...
func (l RunResultList) SortByRequestID() {
	sort.Slice(l, func(i, j int) bool {
		return l[i].RequestID < l[j].RequestID
	})
}

// Percentile ...
func (l RunResultList) Percentile(v float64) time.Duration {
	num := float64(len(l))
	index := int(math.Ceil(v*num/100) - 1)
	if index >= len(l) {
		index = len(l) - 1
	}
	return l[index].Duration
}

// WriteToCSV ...
func (l RunResultList) WriteToCSV(w io.Writer) error {
	writer := csv.NewWriter(w)
	for _, e := range l {
		err := writer.Write([]string{
			strconv.Itoa(e.ThreadID),
			strconv.Itoa(e.RequestID),
			e.StartedAt.UTC().Format(time.RFC3339),
			fmt.Sprintf("%.2f", float64(e.Duration)/1000000.0),
			fmt.Sprintf("%.1f", e.QPS),
			fmt.Sprintf("%.2f", float64(e.BlockedDuration)/1000000.0),
		})
		if err != nil {
			return err
		}
	}
	writer.Flush()
	return nil
}
