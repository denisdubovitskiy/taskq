// Command scheduler демонстрирует функционал планировщика taskq:
// интервальные (AddEvery) и cron-задачи (AddCron), таймзона cron,
// политика catch-up, персистентность расписаний через рестарт,
// защита от двойного запуска через Locker и динамическое управление
// (NextRun, Remove, конфликт определений).
//
// Запуск: go run ./example/scheduler
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/denisdubovitskiy/taskq/adapters/slogadapter"
	filebackend "github.com/denisdubovitskiy/taskq/backends/file"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
	memlocker "github.com/denisdubovitskiy/taskq/lockers/memory"
)

// ticks — общее число выполненных тиков (все экземпляры планировщика).
var ticks int64

func main() {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	backendDir := "./taskq-example-scheduler-results"
	_ = os.RemoveAll(backendDir)

	backend, err := filebackend.New(backendDir)
	if err != nil {
		logger.Error("failed to create backend", slog.String("error", err.Error()))
		os.Exit(1)
	}

	broker := membroker.NewBroker()
	locker := memlocker.NewLocker()

	registry := taskq.NewRegistry()
	tickTask := taskq.NewTask[struct{}, struct{}]("tick")
	if err = taskq.Register(registry, tickTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
		n := atomic.AddInt64(&ticks, 1)
		logger.Info("tick executed", slog.Int("n", int(n)))
		return struct{}{}, nil
	}); err != nil {
		logger.Error("failed to register tick task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	client, err := taskq.NewClient(broker, backend, taskq.WithLogger(slogadapter.NewLogger(logger)))
	if err != nil {
		logger.Error("failed to create client", slog.String("error", err.Error()))
		os.Exit(1)
	}

	worker, err := taskq.NewWorker(registry, broker, backend,
		taskq.WithWorkerLogger(slogadapter.NewLogger(logger)),
	)
	if err != nil {
		logger.Error("failed to create worker", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Воркер работает в фоне на дефолтной очереди.
	go func() {
		if err := worker.Run(ctx, "default"); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker failed", slog.String("error", err.Error()))
		}
	}()

	// runSchedulerAsync запускает планировщик в горутине, логируя сбой
	// (штатная остановка через Stop/отмену ctx возвращает nil).
	runSchedulerAsync := func(name string, run func(context.Context) error) {
		go func() {
			if err := run(ctx); err != nil {
				logger.Error("scheduler failed", slog.String("scheduler", name), slog.String("error", err.Error()))
			}
		}()
	}

	// === 1. Регистрация: интервал, cron, таймзона ===
	s1, err := taskq.NewScheduler(client, locker,
		taskq.WithSchedulerLogger(slogadapter.NewLogger(logger)),
		taskq.WithSchedulerLockTTL(2*time.Second),
	)
	if err != nil {
		logger.Error("failed to create scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}

	tick, err := taskq.AddEvery(ctx, s1, "tick", 500*time.Millisecond, tickTask, struct{}{},
		taskq.WithSubmitOpts(taskq.WithHeader("source", "scheduler")))
	if err != nil {
		logger.Error("failed to add interval schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	hourlyUTC, err := taskq.AddCron(ctx, s1, "hourly-utc", "0 * * * *", tickTask, struct{}{})
	if err != nil {
		logger.Error("failed to add cron schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	hourlyShifted, err := taskq.AddCron(ctx, s1, "hourly-utc-12", "0 * * * *", tickTask, struct{}{},
		taskq.WithCronTimezone(time.FixedZone("UTC-12", -12*3600)))
	if err != nil {
		logger.Error("failed to add timezone cron schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}

	fmt.Printf("tick          next: %s\n", tick.NextRun())
	fmt.Printf("hourly-utc    next: %s  (ночь по UTC)\n", hourlyUTC.NextRun())
	fmt.Printf("hourly-utc-12 next: %s  (ночь по UTC-12 — таймзона сдвигает тик)\n", hourlyShifted.NextRun())
	fmt.Println("cron-задачи в демо не сработают — показан расчет времени следующего срабатывания")

	// === 2. Запуск и второй экземпляр (защита от двойного запуска) ===
	runSchedulerAsync("s1", s1.Run)
	time.Sleep(1200 * time.Millisecond)
	fmt.Printf("одни часы: %d тиков за 1.2s\n", atomic.LoadInt64(&ticks))

	// Второй экземпляр: та же задача с тем же определением — фаза берется
	// из сохраненного документа, тики не дублируются (Locker).
	s2, err := taskq.NewScheduler(client, locker,
		taskq.WithSchedulerLogger(slogadapter.NewLogger(logger)),
		taskq.WithSchedulerLockTTL(2*time.Second),
	)
	if err != nil {
		logger.Error("failed to create second scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if _, err = taskq.AddEvery(ctx, s2, "tick", 500*time.Millisecond, tickTask, struct{}{},
		taskq.WithSubmitOpts(taskq.WithHeader("source", "scheduler"))); err != nil {
		logger.Error("second scheduler must adopt the existing schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	fmt.Println("второй экземпляр зарегистрировал то же расписание — фаза взята из документа")
	runSchedulerAsync("s2", s2.Run)

	before := atomic.LoadInt64(&ticks)
	time.Sleep(1200 * time.Millisecond)
	fmt.Printf("два экземпляра: +%d тиков за 1.2s (двойной запуск исключен lock'ом)\n", atomic.LoadInt64(&ticks)-before)

	// === 3. Конфликт определений ===
	s3, err := taskq.NewScheduler(client, nil)
	if err != nil {
		logger.Error("failed to create third scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	_, err = taskq.AddEvery(ctx, s3, "tick", 900*time.Millisecond, tickTask, struct{}{})
	if errors.Is(err, taskq.ErrScheduleConflict) {
		fmt.Printf("смена интервала отклонена: %v\n", err)
	} else {
		logger.Error("expected schedule conflict", slog.String("error", fmt.Sprint(err)))
		os.Exit(1)
	}

	// === 4. Dynamic removal: Remove also removes the persisted document ===
	if err = s1.Remove("hourly-utc-12"); err != nil {
		logger.Error("failed to remove schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if _, err = backend.GetSchedule(ctx, "hourly-utc-12"); errors.Is(err, taskq.ErrScheduleNotFound) {
		fmt.Println("Remove: расписание удалено вместе с документом")
	} else {
		logger.Error("schedule document must be removed", slog.String("error", fmt.Sprint(err)))
		os.Exit(1)
	}

	// === 5. Persistence: phase survives restart ===
	phase := tick.NextRun()
	if err = s1.Stop(); err != nil {
		logger.Error("failed to stop first scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if err = s2.Stop(); err != nil {
		logger.Error("failed to stop second scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	fmt.Printf("остановлено; сохраненный next: %s\n", phase)

	// «Рестарт» процесса: новый backend из того же каталога, новый клиент.
	restartedBackend, err := filebackend.New(backendDir)
	if err != nil {
		logger.Error("failed to create restarted backend", slog.String("error", err.Error()))
		os.Exit(1)
	}
	restartedClient, err := taskq.NewClient(broker, restartedBackend,
		taskq.WithLogger(slogadapter.NewLogger(logger)))
	if err != nil {
		logger.Error("failed to create restarted client", slog.String("error", err.Error()))
		os.Exit(1)
	}
	s4, err := taskq.NewScheduler(restartedClient, locker,
		taskq.WithSchedulerLogger(slogadapter.NewLogger(logger)),
	)
	if err != nil {
		logger.Error("failed to create restarted scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	tick2, err := taskq.AddEvery(ctx, s4, "tick", 500*time.Millisecond, tickTask, struct{}{},
		taskq.WithSubmitOpts(taskq.WithHeader("source", "scheduler")))
	if err != nil {
		logger.Error("restarted scheduler must adopt the existing schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	fmt.Printf("после рестарта next: %s — фаза сохранена: %v\n", tick2.NextRun(), tick2.NextRun().Equal(phase))

	runSchedulerAsync("s4", s4.Run)
	time.Sleep(1100 * time.Millisecond)
	fmt.Printf("тики продолжаются: всего %d\n", atomic.LoadInt64(&ticks))
	if err = s4.Stop(); err != nil {
		logger.Error("failed to stop restarted scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// === 6. Catch-up: behavior for a missed tick ===
	// гарантированно пропускаем тик: ждем дольше интервала
	time.Sleep(700 * time.Millisecond)
	catches := atomic.LoadInt64(&ticks)

	s5, err := taskq.NewScheduler(restartedClient, nil)
	if err != nil {
		logger.Error("failed to create catch-up scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if _, err = taskq.AddEvery(ctx, s5, "tick", 500*time.Millisecond, tickTask, struct{}{},
		taskq.WithCatchUp(taskq.CatchUpFireOnce)); err != nil {
		logger.Error("failed to add catch-up schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	runSchedulerAsync("s5", s5.Run)
	waitTicks(ctx, catches)
	fmt.Printf("CatchUpFireOnce: +%d догонный тик (политика по умолчанию — 0)\n", atomic.LoadInt64(&ticks)-catches)
	if err = s5.Stop(); err != nil {
		logger.Error("failed to stop catch-up scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}

	time.Sleep(700 * time.Millisecond)

	s6, err := taskq.NewScheduler(restartedClient, nil)
	if err != nil {
		logger.Error("failed to create skip scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}
	tick3, err := taskq.AddEvery(ctx, s6, "tick", 500*time.Millisecond, tickTask, struct{}{})
	if err != nil {
		logger.Error("failed to add skip schedule", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Skip: упущенный тик не выполняется — уже в момент регистрации next
	// сдвинут в будущее, догонного запуска не будет.
	fmt.Printf("CatchUpSkip (по умолчанию): упущенный тик не выполнен — next сдвинут сразу к %s\n",
		tick3.NextRun().Format(time.RFC3339))
	runSchedulerAsync("s6", s6.Run)
	time.Sleep(300 * time.Millisecond)
	if err = s6.Stop(); err != nil {
		logger.Error("failed to stop skip scheduler", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err = worker.Shutdown(ctx); err != nil {
		logger.Error("failed to shutdown worker", slog.String("error", err.Error()))
		os.Exit(1)
	}
	fmt.Println("scheduler demo completed")
}

// waitTicks дожидается, пока число тиков превысит base (не более 2 секунд).
func waitTicks(ctx context.Context, base int64) {
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&ticks) <= base && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}
