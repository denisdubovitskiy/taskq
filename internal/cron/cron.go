// Package cron содержит минимальный парсер стандартных 5-полевых
// cron-выражений: минута час день-месяца месяц день-недели.
//
// Поддерживаются: "*", значения, списки ("1,2,3"), диапазоны ("1-5"),
// шаги ("*/5", "1-5/2"), названия месяцев (jan-dec) и дней недели (sun-sat).
// Дни недели: 0-6, где 0 — воскресенье.
//
// Если ограничены и день месяца, и день недели (оба не "*"),
// срабатывание происходит, когда совпадает хотя бы одно из них
// (семантика Vixie cron).
package cron

import (
	"fmt"
	"strings"
	"time"
)

// maxScanDays — максимальное число дней, которые сканирует Next.
// Покрыть достаточно 4 лет: самый длинный интервал — 29 февраля
// между високосными годами.
const maxScanDays = 4*366 + 1

// field — одно поле cron-выражения: маска допустимых значений
// и флаг, было ли поле звездочкой.
type field struct {
	mask [64]bool
	star bool
}

// Expr — разобранное cron-выражение.
type Expr struct {
	minute field
	hour   field
	dom    field
	month  field
	dow    field
}

// monthNames — названия месяцев jan-dec.
var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

// dowNames — названия дней недели sun-sat.
var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// Parse разбирает 5-полевое cron-выражение.
func Parse(expr string) (*Expr, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d", len(parts))
	}

	minute, err := parseField(parts[0], 0, 59, nil)
	if err != nil {
		return nil, err
	}
	hour, err := parseField(parts[1], 0, 23, nil)
	if err != nil {
		return nil, err
	}
	dom, err := parseField(parts[2], 1, 31, nil)
	if err != nil {
		return nil, err
	}
	month, err := parseField(parts[3], 1, 12, monthNames)
	if err != nil {
		return nil, err
	}
	dow, err := parseField(parts[4], 0, 6, dowNames)
	if err != nil {
		return nil, err
	}

	return &Expr{
		minute: minute,
		hour:   hour,
		dom:    dom,
		month:  month,
		dow:    dow,
	}, nil
}

// Next возвращает следующий момент срабатывания строго после t.
// Секунды и наносекунды результата всегда равны нулю.
func (e *Expr) Next(t time.Time) time.Time {
	// Стартуем со следующей полной минуты после t.
	start := t.Truncate(time.Minute).Add(time.Minute)
	loc := t.Location()
	origin := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, loc)

	for offset := 0; offset <= maxScanDays; offset++ {
		day := origin.AddDate(0, 0, offset)
		if !e.dayMatches(day) {
			continue
		}

		hourFrom, minuteFrom := 0, 0
		if offset == 0 {
			hourFrom, minuteFrom = start.Hour(), start.Minute()
		}

		for h := hourFrom; h < 24; h++ {
			if !e.hour.mask[h] {
				continue
			}
			minuteStart := minuteFrom
			if h > hourFrom {
				minuteStart = 0
			}
			for m := minuteStart; m < 60; m++ {
				if e.minute.mask[m] {
					return time.Date(day.Year(), day.Month(), day.Day(), h, m, 0, 0, loc)
				}
			}
		}
	}

	return time.Time{}
}

// dayMatches проверяет, попадает ли день в поля month, dom и dow.
func (e *Expr) dayMatches(day time.Time) bool {
	if !e.month.mask[int(day.Month())] {
		return false
	}

	domOK := e.dom.mask[day.Day()]
	dowOK := e.dow.mask[int(day.Weekday())]

	switch {
	case e.dom.star && e.dow.star:
		return true
	case e.dom.star:
		return dowOK
	case e.dow.star:
		return domOK
	default:
		return domOK || dowOK
	}
}

// parseField разбирает одно поле: список значений через запятую,
// где каждое значение — "*", число, диапазон "a-b" или опциональный
// шаг "N/M" после "*" или диапазона.
func parseField(part string, lo, hi int, names map[string]int) (field, error) {
	f := field{}

	if part == "" {
		return f, fmt.Errorf("cron: empty field")
	}
	if part == "*" {
		for v := lo; v <= hi; v++ {
			f.mask[v] = true
		}
		f.star = true
		return f, nil
	}

	for _, item := range strings.Split(part, ",") {
		if err := f.addPart(item, lo, hi, names); err != nil {
			return field{}, err
		}
	}
	return f, nil
}

// addPart добавляет одно значение поля (с опциональным шагом) в маску.
func (f *field) addPart(item string, lo, hi int, names map[string]int) error {
	base := item
	step := 1

	if slash := strings.Index(item, "/"); slash >= 0 {
		base = item[:slash]
		stepStr := item[slash+1:]
		var err error
		step, err = parseValue(stepStr, lo, hi, names, item)
		if err != nil {
			return err
		}
		if step < 1 {
			return fmt.Errorf("cron: step must be >= 1, got %d in %q", step, item)
		}
	}

	if base == "*" {
		for v := lo; v <= hi; v += step {
			f.mask[v] = true
		}
		return nil
	}

	start, end, err := parseRange(base, lo, hi, names, item)
	if err != nil {
		return err
	}

	if start > end {
		return fmt.Errorf("cron: range start %d is greater than end %d in %q", start, end, item)
	}
	if !strings.Contains(base, "-") && step > 1 {
		return fmt.Errorf("cron: step is allowed after * or a range, not after %q", item)
	}

	for v := start; v <= end; v += step {
		f.mask[v] = true
	}
	return nil
}

// parseRange возвращает границы значения: число или диапазон "a-b".
func parseRange(base string, lo, hi int, names map[string]int, item string) (int, int, error) {
	if dash := strings.Index(base, "-"); dash >= 0 {
		start, err := parseValue(base[:dash], lo, hi, names, item)
		if err != nil {
			return 0, 0, err
		}
		end, err := parseValue(base[dash+1:], lo, hi, names, item)
		if err != nil {
			return 0, 0, err
		}
		return start, end, nil
	}

	v, err := parseValue(base, lo, hi, names, item)
	if err != nil {
		return 0, 0, err
	}
	return v, v, nil
}

// parseValue разбирает число или название (месяц/день недели).
func parseValue(s string, lo, hi int, names map[string]int, item string) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}

	var v int
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil || !isNumber(s) {
		return 0, fmt.Errorf("cron: invalid value %q in %q", s, item)
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("cron: value %d out of range %d-%d in %q", v, lo, hi, item)
	}
	return v, nil
}

// isNumber проверяет, что строка состоит только из цифр.
func isNumber(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
