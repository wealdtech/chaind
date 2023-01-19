// Copyright © 2023 Weald Technology Limited.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/util"
)

// timeInLocation is a helper to set up tests.
func timeInLocation(input string, location string) time.Time {
	loc, err := time.LoadLocation(location)
	if err != nil {
		panic(err)
	}
	res, err := time.ParseInLocation("2006-01-02T15:04:05", input, loc)
	if err != nil {
		panic(err)
	}
	return res
}

func TestCalendarDuration(t *testing.T) {

	tests := []struct {
		name     string
		duration string
		err      string
		start    time.Time
		// Time series obtained by sequentially adding durations
		nextPeriods []time.Time
		prevPeriods []time.Time
		// Time series obtained by multiplying durations before addition
		// which gives more coherent dates when adding months.
		nextPeriodsByMul []time.Time
		prevPeriodsByMul []time.Time
	}{
		{
			name:     "Empty",
			duration: "",
			err:      "no duration supplied",
		},
		{
			name:     "MalformedDuration",
			duration: "F1D",
			err:      "malformed duration \"F1D\"",
		},
		{
			name:     "BadDuration",
			duration: "P1F",
			err:      "invalid duration \"P1F\"",
		},
		{
			name:     "MonthsOnlyUTC",
			duration: "P3M",
			start:    timeInLocation("2020-01-01T00:00:00", "Etc/UTC"),
			nextPeriods: []time.Time{
				timeInLocation("2020-04-01T00:00:00", "Etc/UTC"),
				timeInLocation("2020-07-01T00:00:00", "Etc/UTC"),
				timeInLocation("2020-10-01T00:00:00", "Etc/UTC"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-10-01T00:00:00", "Etc/UTC"),
				timeInLocation("2019-07-01T00:00:00", "Etc/UTC"),
				timeInLocation("2019-04-01T00:00:00", "Etc/UTC"),
			},
		},
		{
			name:     "MonthsOnlyLondon",
			duration: "P3M",
			start:    timeInLocation("2020-01-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2020-04-01T00:00:00", "Europe/London"),
				timeInLocation("2020-07-01T00:00:00", "Europe/London"),
				timeInLocation("2020-10-01T00:00:00", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-10-01T00:00:00", "Europe/London"),
				timeInLocation("2019-07-01T00:00:00", "Europe/London"),
				timeInLocation("2019-04-01T00:00:00", "Europe/London"),
			},
		},
		{
			name:     "MinutesOnlyUTC",
			duration: "PT3M",
			start:    timeInLocation("2020-01-01T00:00:00", "Etc/UTC"),
			nextPeriods: []time.Time{
				timeInLocation("2020-01-01T00:03:00", "Etc/UTC"),
				timeInLocation("2020-01-01T00:06:00", "Etc/UTC"),
				timeInLocation("2020-01-01T00:09:00", "Etc/UTC"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-12-31T23:57:00", "Etc/UTC"),
				timeInLocation("2019-12-31T23:54:00", "Etc/UTC"),
				timeInLocation("2019-12-31T23:51:00", "Etc/UTC"),
			},
		},
		{
			name:     "MinutesOnlyLondon",
			duration: "PT3M",
			start:    timeInLocation("2020-01-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2020-01-01T00:03:00", "Europe/London"),
				timeInLocation("2020-01-01T00:06:00", "Europe/London"),
				timeInLocation("2020-01-01T00:09:00", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-12-31T23:57:00", "Europe/London"),
				timeInLocation("2019-12-31T23:54:00", "Europe/London"),
				timeInLocation("2019-12-31T23:51:00", "Europe/London"),
			},
		},
		{
			name:     "OddSecondsUTC",
			duration: "PT61S",
			start:    timeInLocation("2020-01-01T00:00:00", "Etc/UTC"),
			nextPeriods: []time.Time{
				timeInLocation("2020-01-01T00:01:01", "Etc/UTC"),
				timeInLocation("2020-01-01T00:02:02", "Etc/UTC"),
				timeInLocation("2020-01-01T00:03:03", "Etc/UTC"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-12-31T23:58:59", "Etc/UTC"),
				timeInLocation("2019-12-31T23:57:58", "Etc/UTC"),
				timeInLocation("2019-12-31T23:56:57", "Etc/UTC"),
			},
		},
		{
			name:     "OddSecondsLondon",
			duration: "PT61S",
			start:    timeInLocation("2020-01-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2020-01-01T00:01:01", "Europe/London"),
				timeInLocation("2020-01-01T00:02:02", "Europe/London"),
				timeInLocation("2020-01-01T00:03:03", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-12-31T23:58:59", "Europe/London"),
				timeInLocation("2019-12-31T23:57:58", "Europe/London"),
				timeInLocation("2019-12-31T23:56:57", "Europe/London"),
			},
		},
		{
			name:     "ThreeYearsUTC",
			duration: "P3Y",
			start:    timeInLocation("2020-01-01T00:00:00", "Etc/UTC"),
			nextPeriods: []time.Time{
				timeInLocation("2023-01-01T00:00:00", "Etc/UTC"),
				timeInLocation("2026-01-01T00:00:00", "Etc/UTC"),
				timeInLocation("2029-01-01T00:00:00", "Etc/UTC"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2017-01-01T00:00:00", "Etc/UTC"),
				timeInLocation("2014-01-01T00:00:00", "Etc/UTC"),
				timeInLocation("2011-01-01T00:00:00", "Etc/UTC"),
			},
		},
		{
			name:     "ThreeYearsLondon",
			duration: "P3Y",
			start:    timeInLocation("2020-01-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2023-01-01T00:00:00", "Europe/London"),
				timeInLocation("2026-01-01T00:00:00", "Europe/London"),
				timeInLocation("2029-01-01T00:00:00", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2017-01-01T00:00:00", "Europe/London"),
				timeInLocation("2014-01-01T00:00:00", "Europe/London"),
				timeInLocation("2011-01-01T00:00:00", "Europe/London"),
			},
		},
		{
			name:     "ThreeOfEverythingUTC",
			duration: "P3Y3M3DT3H3M3S",
			start:    timeInLocation("2020-01-01T00:00:00", "Etc/UTC"),
			nextPeriods: []time.Time{
				timeInLocation("2023-04-04T03:03:03", "Etc/UTC"),
				timeInLocation("2026-07-07T06:06:06", "Etc/UTC"),
				timeInLocation("2029-10-10T09:09:09", "Etc/UTC"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2016-09-28T20:56:57", "Etc/UTC"),
				timeInLocation("2013-06-25T17:53:54", "Etc/UTC"),
				timeInLocation("2010-03-22T14:50:51", "Etc/UTC"),
			},
		},
		{
			name:     "ThreeOfEverythingLondon",
			duration: "P3Y3M3DT3H3M3S",
			start:    timeInLocation("2020-01-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2023-04-04T03:03:03", "Europe/London"),
				timeInLocation("2026-07-07T06:06:06", "Europe/London"),
				timeInLocation("2029-10-10T09:09:09", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2016-09-28T20:56:57", "Europe/London"),
				timeInLocation("2013-06-25T17:53:54", "Europe/London"),
				timeInLocation("2010-03-22T14:50:51", "Europe/London"),
			},
		},
		// This is an edge case and unlikely to be used in practice
		// Because of overflowing, we get different results depending on the order
		// in which we perform the addition operation.
		// Ex: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 32)
		// if days are added first: 2020-02-02 + 1 month = 2020-03-02
		// if months are added first: 2020-02-01 + 32 days = 2020-03-04
		// Our implementation adds days first (prbably more mathematically correct?).
		{
			name:     "LotsOfEverythingUTC",
			duration: "P101Y13M32DT25H61M61S",
			start:    timeInLocation("2020-01-01T00:00:00", "Etc/UTC"),
			nextPeriods: []time.Time{
				timeInLocation("2122-03-03T02:02:01", "Etc/UTC"),
				timeInLocation("2224-05-05T04:04:02", "Etc/UTC"),
				timeInLocation("2326-07-07T06:06:03", "Etc/UTC"),
			},
			nextPeriodsByMul: []time.Time{
				timeInLocation("2122-03-03T02:02:01", "Etc/UTC"),
				timeInLocation("2224-05-08T04:04:02", "Etc/UTC"),
				timeInLocation("2326-07-10T06:06:03", "Etc/UTC"),
			},
			prevPeriods: []time.Time{
				timeInLocation("1917-10-28T21:57:59", "Etc/UTC"),
				timeInLocation("1815-08-25T19:55:58", "Etc/UTC"),
				timeInLocation("1713-06-23T17:53:57", "Etc/UTC"),
			},
			prevPeriodsByMul: []time.Time{
				timeInLocation("1917-10-28T21:57:59", "Etc/UTC"),
				timeInLocation("1815-08-26T19:55:58", "Etc/UTC"),
				timeInLocation("1713-06-23T17:53:57", "Etc/UTC"),
			},
		},
		{
			name:     "LotsOfEverythingLondon",
			duration: "P101Y13M32DT25H61M61S",
			start:    timeInLocation("2020-01-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2122-03-03T02:02:01", "Europe/London"),
				timeInLocation("2224-05-05T04:04:02", "Europe/London"),
				timeInLocation("2326-07-07T06:06:03", "Europe/London"),
			},
			nextPeriodsByMul: []time.Time{
				timeInLocation("2122-03-03T02:02:01", "Europe/London"),
				timeInLocation("2224-05-08T04:04:02", "Europe/London"),
				timeInLocation("2326-07-10T06:06:03", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("1917-10-28T21:57:59", "Europe/London"),
				timeInLocation("1815-08-25T19:55:58", "Europe/London"),
				timeInLocation("1713-06-23T17:53:57", "Europe/London"),
			},
			prevPeriodsByMul: []time.Time{
				timeInLocation("1917-10-28T21:57:59", "Europe/London"),
				timeInLocation("1815-08-26T19:55:58", "Europe/London"),
				timeInLocation("1713-06-23T17:53:57", "Europe/London"),
			},
		},
		{
			name:     "Months1",
			duration: "P1M",
			start:    timeInLocation("2021-02-01T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2021-03-01T00:00:00", "Europe/London"),
				timeInLocation("2021-04-01T00:00:00", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2021-01-01T00:00:00", "Europe/London"),
				timeInLocation("2020-12-01T00:00:00", "Europe/London"),
			},
		},
		// Because Go normalises dates, months addition can be counter-intuitive.
		// Please refer to 	 for details.
		// We reimplement months addition following the logic in https://lubridate.tidyverse.org/reference/mplus.html
		// which matches implemenation of Moment.js and Java
		{
			name:     "EndMonths",
			duration: "P1M",
			start:    timeInLocation("2021-01-31T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2021-02-28T00:00:00", "Europe/London"),
				timeInLocation("2021-03-28T00:00:00", "Europe/London"),
				timeInLocation("2021-04-28T00:00:00", "Europe/London"),
			},
			nextPeriodsByMul: []time.Time{
				timeInLocation("2021-02-28T00:00:00", "Europe/London"),
				timeInLocation("2021-03-31T00:00:00", "Europe/London"),
				timeInLocation("2021-04-30T00:00:00", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2020-12-31T00:00:00", "Europe/London"),
				timeInLocation("2020-11-30T00:00:00", "Europe/London"),
				timeInLocation("2020-10-30T00:00:00", "Europe/London"),
			},
			prevPeriodsByMul: []time.Time{
				timeInLocation("2020-12-31T00:00:00", "Europe/London"),
				timeInLocation("2020-11-30T00:00:00", "Europe/London"),
				timeInLocation("2020-10-31T00:00:00", "Europe/London"),
			},
		},
		{
			name:     "EndMonthsLeapYear",
			duration: "P1M",
			start:    timeInLocation("2020-01-31T00:00:00", "Europe/London"),
			nextPeriods: []time.Time{
				timeInLocation("2020-02-29T00:00:00", "Europe/London"),
				timeInLocation("2020-03-29T00:00:00", "Europe/London"),
				timeInLocation("2020-04-29T00:00:00", "Europe/London"),
			},
			nextPeriodsByMul: []time.Time{
				timeInLocation("2020-02-29T00:00:00", "Europe/London"),
				timeInLocation("2020-03-31T00:00:00", "Europe/London"),
				timeInLocation("2020-04-30T00:00:00", "Europe/London"),
			},
			prevPeriods: []time.Time{
				timeInLocation("2019-12-31T00:00:00", "Europe/London"),
				timeInLocation("2019-11-30T00:00:00", "Europe/London"),
				timeInLocation("2019-10-30T00:00:00", "Europe/London"),
			},
			prevPeriodsByMul: []time.Time{
				timeInLocation("2019-12-31T00:00:00", "Europe/London"),
				timeInLocation("2019-11-30T00:00:00", "Europe/London"),
				timeInLocation("2019-10-31T00:00:00", "Europe/London"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cd, err := util.ParseCalendarDuration(test.duration)
			if test.err != "" {
				require.Error(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.duration, cd.String())
				next := test.start
				prev := test.start
				for i := 0; i < len(test.nextPeriods); i++ {
					next = cd.Increment(next)
					prev = cd.Decrement(prev)
					mulDuration := cd.Mul(i + 1)

					assert.Equal(t, test.nextPeriods[i].String(), next.String())
					assert.Equal(t, test.prevPeriods[i].String(), prev.String())
					if test.nextPeriodsByMul != nil {
						assert.Equal(t, test.nextPeriodsByMul[i].String(), mulDuration.Increment(test.start).String())
						assert.Equal(t, test.prevPeriodsByMul[i].String(), mulDuration.Decrement(test.start).String())
					} else {
						assert.Equal(t, test.nextPeriods[i].String(), mulDuration.Increment(test.start).String())
						assert.Equal(t, test.prevPeriods[i].String(), mulDuration.Decrement(test.start).String())
					}
				}
			}
		})
	}
}

func TestCalendarDurationAdverb(t *testing.T) {
	tests := []struct {
		period string
		adverb string
	}{
		{period: "P1D", adverb: "Daily"},
		{period: "P1M", adverb: "Monthly"},
		{period: "P3M", adverb: "Quarterly"},
		{period: "P6M", adverb: "Half-yearly"},
		{period: "P1Y", adverb: "Yearly"},
		{period: "P1H", adverb: "Hourly"},
		{period: "PT1M", adverb: "PT1M"},
		{period: "P1Y1M", adverb: "P1Y1M"},
		{period: "P2M", adverb: "Two-Monthly"},
		{period: "P20D", adverb: "Twenty-Daily"},
		{period: "P100D", adverb: "100-Daily"},
	}
	for _, test := range tests {
		t.Run(test.period, func(t *testing.T) {
			cd, err := util.ParseCalendarDuration(test.period)
			require.NoError(t, err)
			assert.Equal(t, test.adverb, cd.Adverb())
		})
	}
}

func TestCalendarDurationComponents(t *testing.T) {
	tests := []struct {
		name       string
		duration   string
		components []int
	}{
		{
			name:       "Incrementing",
			duration:   "P1Y2M3DT4H5M6S",
			components: []int{1, 2, 3, 4, 5, 6},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cd, err := util.ParseCalendarDuration(test.duration)
			require.NoError(t, err)
			assert.Equal(t, cd.Years(), test.components[0])
			assert.Equal(t, cd.Months(), test.components[1])
			assert.Equal(t, cd.Days(), test.components[2])
			assert.Equal(t, cd.Hours(), test.components[3])
			assert.Equal(t, cd.Minutes(), test.components[4])
			assert.Equal(t, cd.Seconds(), test.components[5])
		})
	}
}
