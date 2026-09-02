package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParse(t *testing.T, expr string) *Expr {
	t.Helper()

	parsed, err := Parse(expr)
	require.NoError(t, err, "expr %q", expr)
	return parsed
}

func at(y int, mo time.Month, d, h, mi, s int) time.Time {
	return time.Date(y, mo, d, h, mi, s, 0, time.UTC)
}

// TestParse_Next проверяет расчет следующего момента срабатывания
// для основных конструкций cron: звездочки, значения, списки, диапазоны,
// шаги, названия месяцев и дней недели, правило dom/dow.
func TestParse_Next(t *testing.T) {
	t.Parallel()

	// 2026-08-27 — четверг.
	tcs := []struct {
		name string
		expr string
		from time.Time
		want time.Time
	}{
		{
			name: "every minute",
			expr: "* * * * *",
			from: at(2026, 8, 27, 10, 15, 30),
			want: at(2026, 8, 27, 10, 16, 0),
		},
		{
			name: "every minute strictly after boundary",
			expr: "* * * * *",
			from: at(2026, 8, 27, 10, 16, 0),
			want: at(2026, 8, 27, 10, 17, 0),
		},
		{
			name: "at minute zero of every hour",
			expr: "0 * * * *",
			from: at(2026, 8, 27, 10, 15, 0),
			want: at(2026, 8, 27, 11, 0, 0),
		},
		{
			name: "at minute zero strictly after boundary",
			expr: "0 * * * *",
			from: at(2026, 8, 27, 11, 0, 0),
			want: at(2026, 8, 27, 12, 0, 0),
		},
		{
			name: "step on minutes",
			expr: "*/15 * * * *",
			from: at(2026, 8, 27, 10, 14, 59),
			want: at(2026, 8, 27, 10, 15, 0),
		},
		{
			name: "step on minutes past boundary",
			expr: "*/15 * * * *",
			from: at(2026, 8, 27, 10, 15, 0),
			want: at(2026, 8, 27, 10, 30, 0),
		},
		{
			name: "daily at 02:30",
			expr: "30 2 * * *",
			from: at(2026, 8, 27, 3, 0, 0),
			want: at(2026, 8, 28, 2, 30, 0),
		},
		{
			name: "daily at 02:30 same day",
			expr: "30 2 * * *",
			from: at(2026, 8, 27, 2, 29, 0),
			want: at(2026, 8, 27, 2, 30, 0),
		},
		{
			name: "list of minutes",
			expr: "5,20 * * * *",
			from: at(2026, 8, 27, 10, 6, 0),
			want: at(2026, 8, 27, 10, 20, 0),
		},
		{
			name: "list of minutes wraps to next hour",
			expr: "5,20 * * * *",
			from: at(2026, 8, 27, 10, 21, 0),
			want: at(2026, 8, 27, 11, 5, 0),
		},
		{
			name: "weekdays 09:00 from Thursday morning",
			expr: "0 9 * * 1-5",
			from: at(2026, 8, 27, 8, 0, 0),
			want: at(2026, 8, 27, 9, 0, 0),
		},
		{
			name: "weekdays 09:00 from Thursday noon to Friday",
			expr: "0 9 * * 1-5",
			from: at(2026, 8, 27, 10, 0, 0),
			want: at(2026, 8, 28, 9, 0, 0),
		},
		{
			name: "weekdays 09:00 from Saturday to Monday",
			expr: "0 9 * * 1-5",
			from: at(2026, 8, 29, 10, 0, 0),
			want: at(2026, 8, 31, 9, 0, 0),
		},
		{
			name: "yearly on Jan 1",
			expr: "0 0 1 1 *",
			from: at(2026, 8, 27, 0, 0, 0),
			want: at(2027, 1, 1, 0, 0, 0),
		},
		{
			name: "dow name mon",
			expr: "0 12 * * mon",
			from: at(2026, 8, 27, 10, 0, 0),
			want: at(2026, 8, 31, 12, 0, 0),
		},
		{
			name: "dow names case insensitive",
			expr: "15 10 * * MON-FRI",
			from: at(2026, 8, 27, 10, 0, 0),
			want: at(2026, 8, 27, 10, 15, 0),
		},
		{
			name: "step on range",
			expr: "10-40/10 * * * *",
			from: at(2026, 8, 27, 10, 5, 0),
			want: at(2026, 8, 27, 10, 10, 0),
		},
		{
			name: "step on range past last value",
			expr: "10-40/10 * * * *",
			from: at(2026, 8, 27, 10, 41, 0),
			want: at(2026, 8, 27, 11, 10, 0),
		},
		{
			name: "month name jan",
			expr: "0 0 1 jan *",
			from: at(2026, 8, 27, 0, 0, 0),
			want: at(2027, 1, 1, 0, 0, 0),
		},
		{
			name: "dom range next month",
			expr: "0 6 15-20 * *",
			from: at(2026, 8, 25, 7, 0, 0),
			want: at(2026, 9, 15, 6, 0, 0),
		},
		{
			name: "dom and dow restricted match either",
			expr: "0 0 13 * 5",
			from: at(2026, 9, 11, 12, 0, 0),
			want: at(2026, 9, 13, 0, 0, 0),
		},
		{
			name: "dom and dow restricted friday before the 13th",
			expr: "0 0 13 * 5",
			from: at(2026, 8, 27, 0, 0, 1),
			want: at(2026, 8, 28, 0, 0, 0),
		},
		{
			name: "leap day",
			expr: "0 0 29 2 *",
			from: at(2027, 1, 1, 0, 0, 0),
			want: at(2028, 2, 29, 0, 0, 0),
		},
		{
			name: "year rollover",
			expr: "0 0 31 12 *",
			from: at(2026, 12, 31, 1, 0, 0),
			want: at(2027, 12, 31, 0, 0, 0),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			expr := mustParse(t, tc.expr)

			// act
			got := expr.Next(tc.from)

			// assert
			assert.Equal(t, tc.want, got, "expr %q from %s", tc.expr, tc.from)
		})
	}
}

// TestParse_Errors проверяем валидацию выражений: количество полей,
// диапазоны значений, шаг, имена и синтаксис.
func TestParse_Errors(t *testing.T) {
	t.Parallel()

	invalid := []string{
		"",
		"   ",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"-1 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * 32 * *",
		"* * * 0 *",
		"* * * 13 *",
		"* * * * 7",
		"* * * * -1",
		"5-1 * * * *",
		"1-5-7 * * * *",
		"a * * * *",
		"1,,2 * * * *",
		"*/0 * * * *",
		"5/15 * * * *",
		"0 0 * FOO *",
		"0 0 * * MONDAY",
		"0 0 * mar * 1-31",
		"1.5 * * * *",
		"1 2 3 4 5 6 7",
	}

	for _, expr := range invalid {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()

			// act
			parsed, err := Parse(expr)

			// assert
			require.Error(t, err)
			assert.Nil(t, parsed)
		})
	}
}

// TestParse_ValidVariants проверяем, что допустимые варианты выражений
// парсятся без ошибок.
func TestParse_ValidVariants(t *testing.T) {
	t.Parallel()

	valid := []string{
		"* * * * *",
		"0 0 1 1 0",
		"59 23 31 12 6",
		"*/1 * * * *",
		"0 0 * * 0-6",
		"0 0 1-15 * *",
		"0 0 * jan,jul *",
		"0 0 * * sun,sat",
		"1-59/2 0-23/3 1-31/2 1-12/3 0-6/2",
	}

	for _, expr := range valid {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()

			// act
			parsed, err := Parse(expr)

			// assert
			require.NoError(t, err, "expr %q", expr)
			require.NotNil(t, parsed)
		})
	}
}
