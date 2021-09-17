package loadtest

import (
	"bytes"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestSortPercentile(t *testing.T) {
	l := RunResultList([]RunResult{
		{Duration: 4 * time.Second},
		{Duration: 3 * time.Second},
		{Duration: 2 * time.Second},
		{Duration: 2 * time.Second},
		{Duration: 5 * time.Second},
	})
	l.Sort()
	result := l.Percentile(80)
	assert.Equal(t, 4*time.Second, result)
}

func TestSortPercentile_TooBig(t *testing.T) {
	l := RunResultList([]RunResult{
		{Duration: 4 * time.Second},
		{Duration: 3 * time.Second},
		{Duration: 2 * time.Second},
		{Duration: 2 * time.Second},
		{Duration: 5 * time.Second},
	})
	l.Sort()
	result := l.Percentile(101)
	assert.Equal(t, 5*time.Second, result)
}

func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func mustParseNull(s string) sql.NullTime {
	return sql.NullTime{
		Valid: true,
		Time:  mustParse(s),
	}
}

func TestWriteToCSV(t *testing.T) {
	l := RunResultList([]RunResult{
		{
			ThreadID:        10,
			RequestID:       20,
			StartedAt:       mustParse("2021-09-16T10:20:30+07:00"),
			Duration:        4 * time.Second,
			QPS:             20,
			BlockedDuration: 10 * time.Millisecond,
		},
		{
			ThreadID:        11,
			RequestID:       21,
			StartedAt:       mustParse("2021-09-18T10:20:30+07:00"),
			Duration:        328 * time.Microsecond,
			QPS:             30,
			BlockedDuration: 250 * time.Microsecond,
		},
	})

	var buf bytes.Buffer
	err := l.WriteToCSV(&buf)
	assert.Equal(t, nil, err)
	expected := strings.TrimLeft(`
10,20,2021-09-16T03:20:30Z,4000.00,20.0,10.00
11,21,2021-09-18T03:20:30Z,0.33,30.0,0.25
`, " \r\n\t")
	assert.Equal(t, expected, buf.String())
}
