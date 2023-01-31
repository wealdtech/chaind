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

package util

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Time units for calculating durations in time.Duration
const (
	Day   = 24 * time.Hour
	Month = 30 * Day
	Year  = 365 * Day
)

// CalendarDuration represents a calendar-based duration.
type CalendarDuration struct {
	seconds int
	minutes int
	hours   int
	days    int
	months  int
	years   int
}

// calendarDurationRegex is an ISO-8601 date
var calendarDurationRegex = regexp.MustCompile(`P([\d\.]+Y)?([\d\.]+M)?([\d\.]+D)?T?([\d\.]+H)?([\d\.]+M)?([\d\.]+?S)?`)

// ParseCalendarDuration parses a duration string and returns a calendar duration.
func ParseCalendarDuration(duration string) (*CalendarDuration, error) {
	if duration == "" {
		return nil, errors.New("no duration supplied")
	}

	matches := calendarDurationRegex.FindStringSubmatch(duration)
	if len(matches) == 0 {
		return nil, fmt.Errorf("malformed duration %q", duration)
	}
	if matches[1] == "" &&
		matches[2] == "" &&
		matches[3] == "" &&
		matches[4] == "" &&
		matches[5] == "" &&
		matches[6] == "" {
		return nil, fmt.Errorf("invalid duration %q", duration)
	}

	return &CalendarDuration{
		years:   parseDurationPart(matches[1]),
		months:  parseDurationPart(matches[2]),
		days:    parseDurationPart(matches[3]),
		hours:   parseDurationPart(matches[4]),
		minutes: parseDurationPart(matches[5]),
		seconds: parseDurationPart(matches[6]),
	}, nil
}

// MustParseCalendarDuration parses a duration string, panicking on error.
func MustParseCalendarDuration(duration string) *CalendarDuration {
	dur, err := ParseCalendarDuration(duration)
	if err != nil {
		panic(err)
	}
	return dur
}

func parseDurationPart(value string) int {
	if len(value) != 0 {
		if val, err := strconv.ParseInt(value[:len(value)-1], 10, 32); err == nil {
			return int(val)
		}
	}
	return 0
}

// Increment increments the date by the duration.
func (d *CalendarDuration) Increment(date time.Time) time.Time {
	incrementedTime := time.Date(
		date.Year()+d.years,
		date.Month(),
		date.Day()+d.days,
		date.Hour()+d.hours,
		date.Minute()+d.minutes,
		date.Second()+d.seconds,
		0,
		date.Location())
	return shiftMonths(incrementedTime, d.months)
}

// Decrement decrements the date by the duration.
func (d *CalendarDuration) Decrement(date time.Time) time.Time {
	decrementedTime := time.Date(
		date.Year()-d.years,
		date.Month(),
		date.Day()-d.days,
		date.Hour()-d.hours,
		date.Minute()-d.minutes,
		date.Second()-d.seconds,
		0,
		date.Location())
	return shiftMonths(decrementedTime, -d.months)
}

// Seconds returns this duration's seconds.
func (d *CalendarDuration) Seconds() int {
	return d.seconds
}

// Minutes returns this duration's minutes.
func (d *CalendarDuration) Minutes() int {
	return d.minutes
}

// Hours returns this duration's hours.
func (d *CalendarDuration) Hours() int {
	return d.hours
}

// Days returns this duration's days.
func (d *CalendarDuration) Days() int {
	return d.days
}

// Months returns this duration's months.
func (d *CalendarDuration) Months() int {
	return d.months
}

// Years returns this duration's years.
func (d *CalendarDuration) Years() int {
	return d.years
}

// String returns the ISO-8601 period string.
func (d *CalendarDuration) String() string {
	res := "P"
	if d.years > 0 {
		res = fmt.Sprintf("%s%dY", res, d.years)
	}
	if d.months > 0 {
		res = fmt.Sprintf("%s%dM", res, d.months)
	}
	if d.days > 0 {
		res = fmt.Sprintf("%s%dD", res, d.days)
	}
	if d.hours > 0 || d.minutes > 0 || d.seconds > 0 {
		res = fmt.Sprintf("%sT", res)
	}
	if d.hours > 0 {
		res = fmt.Sprintf("%s%dH", res, d.hours)
	}
	if d.minutes > 0 {
		res = fmt.Sprintf("%s%dM", res, d.minutes)
	}
	if d.seconds > 0 {
		res = fmt.Sprintf("%s%dS", res, d.seconds)
	}
	return res
}

// Adverb converts CalendarDuration to an English adverb (Daily / Yearly, etc)
func (d *CalendarDuration) Adverb() string {
	str := d.String()
	singlePeriod, _ := regexp.MatchString("P[T]*\\d+\\w$", str)
	if !singlePeriod {
		return str
	}
	var number int
	var period string
	switch {
	case d.years > 0:
		number = d.years
		period = "Yearly"
	case d.months == 6:
		number = 1
		period = "Half-yearly"
	case d.months == 3:
		number = 1
		period = "Quarterly"
	case d.months > 0:
		number = d.months
		period = "Monthly"
	case d.days > 0:
		number = d.days
		period = "Daily"
	case d.hours > 0:
		number = d.hours
		period = "Hourly"
	default:
		return str
	}

	if number > 20 {
		return fmt.Sprintf("%d-%s", number, period)
	}

	numbers2words := []string{
		"", "", "Two-", "Three-", "Four-", "Five-", "Six-",
		"Seven-", "Eight-", "Nine-", "Ten-",
		"Eleven-", "Twelve-", "Thirteen-", "Fourteen-", "Fifteen-",
		"Sixteen-", "Seventeen-", "Eighteen-", "Nineteen-", "Twenty-",
	}

	return fmt.Sprintf("%s%s", numbers2words[number], period)
}

// ToDuration converts CalendarDuration to time.Duration
func (d *CalendarDuration) ToDuration() time.Duration {
	return time.Duration(d.years)*Year +
		time.Duration(d.months)*Month +
		time.Duration(d.days)*Day +
		time.Duration(d.hours)*time.Hour +
		time.Duration(d.minutes)*time.Minute +
		time.Duration(d.seconds)*time.Second
}

// Mul performs multiplication for CalendarDuration
func (d *CalendarDuration) Mul(times int) *CalendarDuration {
	return &CalendarDuration{
		years:   d.years * times,
		months:  d.months * times,
		days:    d.days * times,
		hours:   d.hours * times,
		minutes: d.minutes * times,
		seconds: d.seconds * times,
	}
}

// shiftMonths correctly add months without overflowing
func shiftMonths(date time.Time, months int) time.Time {
	t := date.AddDate(0, months, 0)
	if t.Day() < date.Day() {
		t = time.Date(t.Year(), t.Month(), 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location()).AddDate(0, 0, -1)
	}
	return t
}
