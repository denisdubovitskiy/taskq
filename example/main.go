package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/denisdubovitskiy/taskq/adapters/slogadapter"
	filebackend "github.com/denisdubovitskiy/taskq/backends/file"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
)

// AddArgs — аргументы задачи сложения.
type AddArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

// AddResult — результат задачи сложения.
type AddResult struct {
	Sum int `json:"sum"`
}

// MultiplyArgs — аргументы задачи умножения.
type MultiplyArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

// MultiplyResult — результат задачи умножения.
type MultiplyResult struct {
	Product int `json:"product"`
}

// FailArgs — аргументы задачи, которая всегда падает.
type FailArgs struct {
	Message string `json:"message"`
}

// ScaleArgs — аргументы задачи масштабирования (цепочка, шаг 1).
type ScaleArgs struct {
	Value  int `json:"value"`
	Factor int `json:"factor"`
}

// ScaleResult — результат задачи масштабирования.
type ScaleResult struct {
	Value int `json:"value"`
}

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	broker := membroker.NewBroker()

	backendDir := "./taskq-example-results"
	_ = os.RemoveAll(backendDir)
	backend, err := filebackend.New(backendDir)
	if err != nil {
		logger.Error("failed to create backend", slog.String("error", err.Error()))
		os.Exit(1)
	}

	client, err := taskq.NewClient(
		broker,
		backend,
		taskq.WithLogger(slogadapter.NewLogger(logger)),
		taskq.WithTracer(slogadapter.NewTracer(logger)),
		taskq.WithMeter(slogadapter.NewMeter(logger)),
	)
	if err != nil {
		logger.Error("failed to create client", slog.String("error", err.Error()))
		os.Exit(1)
	}

	registry := taskq.NewRegistry()

	addTask := taskq.NewTask[AddArgs, AddResult]("add")
	if err := taskq.Register(registry, addTask, func(ctx context.Context, args AddArgs) (AddResult, error) {
		logger.Info("executing add", slog.Int("a", args.A), slog.Int("b", args.B))
		return AddResult{Sum: args.A + args.B}, nil
	}); err != nil {
		logger.Error("failed to register add task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	multiplyTask := taskq.NewTask[MultiplyArgs, MultiplyResult]("multiply")
	if err := taskq.Register(registry, multiplyTask, func(ctx context.Context, args MultiplyArgs) (MultiplyResult, error) {
		logger.Info("executing multiply", slog.Int("a", args.A), slog.Int("b", args.B))
		return MultiplyResult{Product: args.A * args.B}, nil
	}); err != nil {
		logger.Error("failed to register multiply task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	failTask := taskq.NewTask[FailArgs, struct{}]("fail")
	attempts := 0
	if err := taskq.Register(registry, failTask, func(ctx context.Context, args FailArgs) (struct{}, error) {
		attempts++
		logger.Info("executing failing task", slog.String("message", args.Message), slog.Int("attempt", attempts))
		return struct{}{}, errors.New(args.Message)
	}); err != nil {
		logger.Error("failed to register fail task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logTask := taskq.NewTask[AddArgs, struct{}]("log")
	if err := taskq.Register(registry, logTask, func(ctx context.Context, args AddArgs) (struct{}, error) {
		logger.Info("executing log task", slog.Int("a", args.A), slog.Int("b", args.B))
		return struct{}{}, nil
	}); err != nil {
		logger.Error("failed to register log task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slowTask := taskq.NewTask[AddArgs, AddResult]("slow")
	if err := taskq.Register(registry, slowTask, func(ctx context.Context, args AddArgs) (AddResult, error) {
		logger.Info("executing slow task (3s)", slog.Int("a", args.A), slog.Int("b", args.B))
		select {
		case <-time.After(3 * time.Second):
			return AddResult{Sum: args.A + args.B}, nil
		case <-ctx.Done():
			return AddResult{}, ctx.Err()
		}
	}); err != nil {
		logger.Error("failed to register slow task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	flakyTask := taskq.NewTask[AddArgs, AddResult]("flaky")
	flakyCalls := 0
	if err := taskq.Register(registry, flakyTask, func(ctx context.Context, args AddArgs) (AddResult, error) {
		flakyCalls++
		if flakyCalls <= 2 {
			return AddResult{}, fmt.Errorf("flaky: transient failure (call %d)", flakyCalls)
		}
		return AddResult{Sum: args.A + args.B}, nil
	}); err != nil {
		logger.Error("failed to register flaky task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	scaleTask := taskq.NewTask[ScaleArgs, ScaleResult]("scale")
	if err := taskq.Register(registry, scaleTask, func(ctx context.Context, args ScaleArgs) (ScaleResult, error) {
		logger.Info("executing scale task", slog.Int("value", args.Value), slog.Int("factor", args.Factor))
		return ScaleResult{Value: args.Value * args.Factor}, nil
	}); err != nil {
		logger.Error("failed to register scale task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	shiftTask := taskq.NewTask[ScaleResult, ScaleResult]("shift")
	if err := taskq.Register(registry, shiftTask, func(ctx context.Context, args ScaleResult) (ScaleResult, error) {
		logger.Info("executing shift task", slog.Int("value", args.Value))
		return ScaleResult{Value: args.Value + 10}, nil
	}); err != nil {
		logger.Error("failed to register shift task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	sumAllTask := taskq.NewTask[[]AddResult, AddResult]("sum-all")
	if err := taskq.Register(registry, sumAllTask, func(ctx context.Context, args []AddResult) (AddResult, error) {
		total := 0
		for _, r := range args {
			total += r.Sum
		}
		logger.Info("executing sum-all task", slog.Int("total", total))
		return AddResult{Sum: total}, nil
	}); err != nil {
		logger.Error("failed to register sum-all task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// reportTask демонстрарует именованные очереди: сабмитится в очередь
	// "reports" через WithQueue, воркер потребляет её вместе с дефолтной.
	reportTask := taskq.NewTask[AddArgs, AddResult]("report")
	if err := taskq.Register(registry, reportTask, func(ctx context.Context, args AddArgs) (AddResult, error) {
		logger.Info("executing report task (queue: reports)", slog.Int("a", args.A), slog.Int("b", args.B))
		return AddResult{Sum: args.A + args.B}, nil
	}); err != nil {
		logger.Error("failed to register report task", slog.String("error", err.Error()))
		os.Exit(1)
	}

	worker, err := taskq.NewWorker(
		registry,
		broker,
		backend,
		taskq.WithConcurrency(4),
		// «Тяжёлые» slow-задачи — не более одной одновременно (within общего пула).
		taskq.WithTaskConcurrency("slow", 1),
		taskq.WithWorkerLogger(slogadapter.NewLogger(logger)),
		taskq.WithWorkerTracer(slogadapter.NewTracer(logger)),
		taskq.WithWorkerMeter(slogadapter.NewMeter(logger)),
		taskq.WithPreExecuteHook(func(ctx context.Context, job *taskq.Job) error {
			logger.Info("pre-execute hook", slog.String("job_id", job.ID), slog.String("task", job.Name))
			return nil
		}),
		taskq.WithPostExecuteHook(func(ctx context.Context, job *taskq.Job, state taskq.State, err error) {
			if err != nil {
				logger.Error("post-execute hook: failed", slog.String("job_id", job.ID), slog.String("state", string(state)), slog.String("error", err.Error()))
				return
			}
			logger.Info("post-execute hook: success", slog.String("job_id", job.ID), slog.String("state", string(state)))
		}),
		taskq.WithErrorHandler(func(ctx context.Context, job *taskq.Job, err error) error {
			logger.Error("error handler", slog.String("job_id", job.ID), slog.String("error", err.Error()))
			return nil
		}),
		taskq.WithOnDeadHook(func(ctx context.Context, job *taskq.Job, lastErr error) {
			logger.Error("dead letter hook", slog.String("job_id", job.ID), slog.String("task", job.Name), slog.String("error", lastErr.Error()))
		}),
	)
	if err != nil {
		logger.Error("failed to create worker", slog.String("error", err.Error()))
		os.Exit(1)
	}

	go func() {
		// Воркер работает на дефолтной и именованной очередях одновременно.
		if err := worker.RunQueues(ctx, "default", "reports"); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker failed", slog.String("error", err.Error()))
		}
	}()

	if err := runExamples(ctx, client, worker, addTask, multiplyTask, failTask, logTask, scaleTask, shiftTask, sumAllTask, slowTask, flakyTask, reportTask); err != nil {
		logger.Error("examples failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("all examples completed, stopping worker gracefully")

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := worker.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown worker gracefully", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("worker stopped gracefully")
}

func runExamples(
	ctx context.Context,
	client *taskq.Client,
	worker *taskq.Worker,
	addTask *taskq.Task[AddArgs, AddResult],
	multiplyTask *taskq.Task[MultiplyArgs, MultiplyResult],
	failTask *taskq.Task[FailArgs, struct{}],
	logTask *taskq.Task[AddArgs, struct{}],
	scaleTask *taskq.Task[ScaleArgs, ScaleResult],
	shiftTask *taskq.Task[ScaleResult, ScaleResult],
	sumAllTask *taskq.Task[[]AddResult, AddResult],
	slowTask *taskq.Task[AddArgs, AddResult],
	flakyTask *taskq.Task[AddArgs, AddResult],
	reportTask *taskq.Task[AddArgs, AddResult],
) error {
	fmt.Println("\n=== Example 1: simple add ===")
	addFuture, err := taskq.Submit(ctx, client, addTask, AddArgs{A: 10, B: 32})
	if err != nil {
		return fmt.Errorf("submit add: %w", err)
	}
	addResult, err := addFuture.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get add result: %w", err)
	}
	fmt.Printf("10 + 32 = %d\n", addResult.Sum)

	fmt.Println("\n=== Example 2: delayed multiply ===")
	multiplyFuture, err := taskq.Submit(
		ctx,
		client,
		multiplyTask,
		MultiplyArgs{A: 6, B: 7},
		taskq.WithDelay(2*time.Second),
	)
	if err != nil {
		return fmt.Errorf("submit multiply: %w", err)
	}
	multiplyResult, err := multiplyFuture.GetWithTimeout(ctx, 10*time.Second)
	if err != nil {
		return fmt.Errorf("get multiply result: %w", err)
	}
	fmt.Printf("6 * 7 = %d (after delay)\n", multiplyResult.Product)

	fmt.Println("\n=== Example 3: retry policy on failing task ===")
	_, err = taskq.Submit(
		ctx,
		client,
		failTask,
		FailArgs{Message: "expected failure"},
		taskq.WithRetry(taskq.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     2 * time.Second,
			Multiplier:   2.0,
		}),
	)
	if err != nil {
		return fmt.Errorf("submit fail task: %w", err)
	}

	time.Sleep(5 * time.Second)

	fmt.Println("\n=== Example 4: fire-and-forget ===")
	if err := taskq.SubmitOneWay(ctx, client, logTask, AddArgs{A: 1, B: 2}); err != nil {
		return fmt.Errorf("submit one way: %w", err)
	}

	time.Sleep(1 * time.Second)

	fmt.Println("\n=== Example 5: group ===")
	group, err := taskq.SubmitGroup(ctx, client, addTask, []AddArgs{
		{A: 1, B: 2},
		{A: 3, B: 4},
		{A: 5, B: 6},
	})
	if err != nil {
		return fmt.Errorf("submit group: %w", err)
	}
	groupResult, err := group.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get group result: %w", err)
	}
	for i, r := range groupResult.Results {
		fmt.Printf("group task %d: %d\n", i, r.Sum)
	}
	fmt.Printf("group all succeeded: %v\n", groupResult.AllSucceeded())

	fmt.Println("\n=== Example 6: chain ===")
	chain, err := taskq.NewChain(client)
	if err != nil {
		return fmt.Errorf("new chain: %w", err)
	}
	chainBuilder := taskq.Add(
		taskq.Add(chain, scaleTask, ScaleArgs{Value: 21, Factor: 2}),
		shiftTask,
	)
	chainFuture, err := chainBuilder.Send(ctx)
	if err != nil {
		return fmt.Errorf("send chain: %w", err)
	}
	fmt.Printf("chain steps: %d\n", len(chainBuilder.StepIDs()))
	chainResult, err := chainFuture.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get chain result: %w", err)
	}
	fmt.Printf("chain: 21*2+10 = %d\n", chainResult.Value)

	fmt.Println("\n=== Example 7: chord ===")
	chordFuture, err := taskq.SubmitChord(ctx, client, addTask, []AddArgs{
		{A: 1, B: 2},
		{A: 3, B: 4},
		{A: 5, B: 6},
	}, sumAllTask)
	if err != nil {
		return fmt.Errorf("submit chord: %w", err)
	}
	chordResult, err := chordFuture.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get chord result: %w", err)
	}
	fmt.Printf("chord: 3+7+11 = %d\n", chordResult.Sum)

	fmt.Println("\n=== Example 8: task timeout ===")
	slowFuture, err := taskq.Submit(ctx, client, slowTask, AddArgs{A: 1, B: 1}, taskq.WithTimeout(200*time.Millisecond))
	if err != nil {
		return fmt.Errorf("submit slow task: %w", err)
	}
	if _, err = slowFuture.GetWithTimeout(ctx, 5*time.Second); err != nil {
		fmt.Printf("timeout caught: %v\n", err)
	}

	fmt.Println("\n=== Example 9: cancel in-flight job ===")
	cancelFuture, err := taskq.Submit(ctx, client, slowTask, AddArgs{A: 2, B: 2})
	if err != nil {
		return fmt.Errorf("submit slow task: %w", err)
	}
	if _, err = waitForState(ctx, client, cancelFuture.ID(), taskq.StateStarted); err != nil {
		return fmt.Errorf("wait for started: %w", err)
	}
	if !worker.Cancel(cancelFuture.ID()) {
		return fmt.Errorf("worker.Cancel returned false")
	}
	if _, err = cancelFuture.GetWithTimeout(ctx, 5*time.Second); err != nil {
		fmt.Printf("canceled: %v\n", err)
	}

	fmt.Println("\n=== Example 10: dead letter + rescue ===")
	flakyFuture, err := taskq.Submit(
		ctx,
		client,
		flakyTask,
		AddArgs{A: 20, B: 22},
		taskq.WithRetry(taskq.RetryPolicy{
			MaxAttempts:  2,
			InitialDelay: 100 * time.Millisecond,
		}),
	)
	if err != nil {
		return fmt.Errorf("submit flaky task: %w", err)
	}
	job, err := waitForState(ctx, client, flakyFuture.ID(), taskq.StateDead)
	if err != nil {
		return fmt.Errorf("wait for dead: %w", err)
	}
	fmt.Printf("dead letter: state=%s last_error=%s\n", job.State, job.Error)
	if err = client.Rescue(ctx, flakyFuture.ID()); err != nil {
		return fmt.Errorf("rescue flaky task: %w", err)
	}
	rescued, err := flakyFuture.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get rescued result: %w", err)
	}
	fmt.Printf("rescued: 20 + 22 = %d\n", rescued.Sum)

	fmt.Println("\n=== Example 11: inspect and list jobs ===")
	if err := taskq.SubmitOneWay(ctx, client, logTask, AddArgs{A: 5, B: 5}, taskq.WithJobID("demo-inspect")); err != nil {
		return fmt.Errorf("submit inspect demo: %w", err)
	}
	if _, err = waitForState(ctx, client, "demo-inspect", taskq.StateSuccess); err != nil {
		return fmt.Errorf("wait for inspect demo: %w", err)
	}
	listed, err := client.List(ctx, taskq.ListQuery{Task: "log", Limit: 3})
	if err != nil {
		return fmt.Errorf("list jobs: %w", err)
	}
	for _, item := range listed.Items {
		fmt.Printf("job %s: task=%s state=%s\n", item.ID, item.Task, item.State)
	}

	fmt.Println("\n=== Example 12: idempotent submit ===")
	idem1, err := taskq.Submit(ctx, client, addTask, AddArgs{A: 100, B: 1}, taskq.WithJobID("demo-idempotent"))
	if err != nil {
		return fmt.Errorf("submit idempotent: %w", err)
	}
	idem2, err := taskq.Submit(ctx, client, addTask, AddArgs{A: 100, B: 1}, taskq.WithJobID("demo-idempotent"))
	if err != nil {
		return fmt.Errorf("submit idempotent duplicate: %w", err)
	}
	fmt.Printf("same job id on duplicate submit: %v\n", idem1.ID() == idem2.ID())
	idemResult, err := idem1.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get idempotent result: %w", err)
	}
	fmt.Printf("100 + 1 = %d\n", idemResult.Sum)

	fmt.Println("\n=== Example 13: named queue via WithQueue ===")
	reportFuture, err := taskq.Submit(ctx, client, reportTask, AddArgs{A: 7, B: 8}, taskq.WithQueue("reports"))
	if err != nil {
		return fmt.Errorf("submit report: %w", err)
	}
	reportResult, err := reportFuture.GetWithTimeout(ctx, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get report result: %w", err)
	}
	fmt.Printf("job routed to \"reports\" queue and processed: 7 + 8 = %d\n", reportResult.Sum)

	fmt.Println("\n=== Example 14: cancel before start (Client.Cancel) ===")
	// Task.WithQueue — привязка очереди к задаче строителем: очередь «cold»
	// никто не потребляет, поэтому задача гарантированно не стартует.
	coldTask := taskq.NewTask[AddArgs, AddResult]("cold").WithQueue("cold")
	coldFuture, err := taskq.Submit(ctx, client, coldTask, AddArgs{A: 1, B: 1})
	if err != nil {
		return fmt.Errorf("submit cold: %w", err)
	}
	if err = client.Cancel(ctx, coldFuture.ID()); err != nil {
		return fmt.Errorf("cancel cold: %w", err)
	}
	canceledJob, err := waitForState(ctx, client, coldFuture.ID(), taskq.StateCanceled)
	if err != nil {
		return fmt.Errorf("wait for canceled: %w", err)
	}
	fmt.Printf("state: %s\n", canceledJob.State)
	if _, err = coldFuture.GetWithTimeout(ctx, time.Second); err != nil && errors.Is(err, taskq.ErrJobCanceled) {
		fmt.Printf("future returned: %v\n", err)
	}

	fmt.Println("\n=== Example 15: deadline and headers ===")
	deadlineFuture, err := taskq.Submit(
		ctx,
		client,
		slowTask,
		AddArgs{A: 1, B: 1},
		taskq.WithDeadline(time.Now().UTC().Add(300*time.Millisecond)),
	)
	if err != nil {
		return fmt.Errorf("submit deadline job: %w", err)
	}
	if _, err = deadlineFuture.GetWithTimeout(ctx, 10*time.Second); err != nil {
		fmt.Printf("deadline exceeded: %v\n", err)
	}
	headerFuture, err := taskq.Submit(ctx, client, logTask, AddArgs{A: 2, B: 3},
		taskq.WithHeader("team", "demo"), taskq.WithHeader("trace_id", "abc-123"))
	if err != nil {
		return fmt.Errorf("submit header job: %w", err)
	}
	time.Sleep(500 * time.Millisecond)
	headerJob, err := client.Inspect(ctx, headerFuture.ID())
	if err != nil {
		return fmt.Errorf("inspect header job: %w", err)
	}
	fmt.Printf("headers: team=%s trace_id=%s\n", headerJob.Headers["team"], headerJob.Headers["trace_id"])

	fmt.Println("\n=== Example 16: Future.Touch, list pagination, Delete ===")
	touchFuture, err := taskq.Submit(ctx, client, slowTask, AddArgs{A: 4, B: 5})
	if err != nil {
		return fmt.Errorf("submit touch job: %w", err)
	}
	// Touch — неблокирующая проверка состояния (в отличие от Get).
	for i := 0; i < 3; i++ {
		if _, state, err := touchFuture.Touch(ctx); err == nil && state != nil {
			fmt.Printf("touch: state=%s\n", state.State)
		}
		time.Sleep(700 * time.Millisecond)
	}
	touchResult, err := touchFuture.GetWithTimeout(ctx, 10*time.Second)
	if err != nil {
		return fmt.Errorf("get touch result: %w", err)
	}
	fmt.Printf("touch job done: 4 + 5 = %d\n", touchResult.Sum)

	// Пагинация списка задач курсором.
	for i := 0; i < 3; i++ {
		if err = taskq.SubmitOneWay(ctx, client, logTask, AddArgs{A: i, B: i}); err != nil {
			return fmt.Errorf("submit page job: %w", err)
		}
	}
	time.Sleep(500 * time.Millisecond)
	page, err := client.List(ctx, taskq.ListQuery{Task: "log", Limit: 2})
	if err != nil {
		return fmt.Errorf("list page 1: %w", err)
	}
	fmt.Printf("page 1: %d jobs, next cursor: %q\n", len(page.Items), page.Cursor)
	nextPage, err := client.List(ctx, taskq.ListQuery{Task: "log", Limit: 2, Cursor: page.Cursor})
	if err != nil {
		return fmt.Errorf("list page 2: %w", err)
	}
	fmt.Printf("page 2: %d jobs, next cursor: %q\n", len(nextPage.Items), nextPage.Cursor)

	// Delete — удаление задачи из backend.
	if err = client.Delete(ctx, touchFuture.ID()); err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	if _, err = client.Inspect(ctx, touchFuture.ID()); err != nil && errors.Is(err, taskq.ErrJobNotFound) {
		fmt.Printf("deleted job: inspect now returns %v\n", err)
	}

	return nil
}

// waitForState дожидается, пока задача перейдет в состояние want (не более 5 секунд).
func waitForState(ctx context.Context, client *taskq.Client, jobID string, want taskq.State) (*taskq.Job, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := client.Inspect(ctx, jobID)
		if err == nil && job.State == want {
			return job, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	job, err := client.Inspect(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("inspect job %s: %w", jobID, err)
	}
	return nil, fmt.Errorf("job %s is in state %s, want %s", jobID, job.State, want)
}
