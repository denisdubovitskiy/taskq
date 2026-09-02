# taskq contrib/postgresql — цели разработки и интеграционных тестов.
#
# Интеграционные тесты требуют PostgreSQL. По умолчанию поднимаем свой через
# docker compose (порт 5432, см. docker-compose.yml); DSN переопределяется
# переменной окружения TASKQ_TEST_PG_DSN (или PG_DSN ниже).
#
# Быстрый старт:
#   make test-docker   # up + тесты + down — самодостаточно, ничего не оставляет

.DEFAULT_GOAL := help

COMPOSE := docker compose

# DSN тестовой PostgreSQL. Если TASKQ_TEST_PG_DSN уже задан в окружении —
# уважаем его; иначе дефолт совпадает с маппингом docker-compose.yml.
ifndef TASKQ_TEST_PG_DSN
PG_DSN := postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable
else
PG_DSN := $(TASKQ_TEST_PG_DSN)
endif
export TASKQ_TEST_PG_DSN = $(PG_DSN)

.PHONY: help up wait down test test-race test-local test-docker vet lint fmt tidy build clean

# Справка (по умолчанию)
help:
	@echo "taskq contrib/postgresql"
	@echo ""
	@echo "Цели:"
	@echo "  make test         — интеграционные тесты (нужна PostgreSQL, дефолт из docker-compose)"
	@echo "  make test-race    — тесты с race detector"
	@echo "  make up           — поднять PostgreSQL (docker compose, ждать готовности)"
	@echo "  make wait         — дождаться готовности PostgreSQL (pg_isready)"
	@echo "  make down         — остановить PostgreSQL (docker compose)"
	@echo "  make test-local   — up + тесты (PostgreSQL остается запущенной)"
	@echo "  make test-docker  — up + тесты + down (самодостаточно)"
	@echo "  make vet          — go vet"
	@echo "  make lint         — go vet + staticcheck (нужен staticcheck)"
	@echo "  make fmt          — gofmt -w ."
	@echo "  make tidy         — go mod tidy"
	@echo "  make build        — собрать пакеты"
	@echo "  make clean        — down + go clean -testcache"

# Поднять тестовую PostgreSQL и дождаться, пока она станет ready.
up:
	$(COMPOSE) up -d --wait

# Дождаться готовности PostgreSQL (периодический pg_isready, до ~30 c).
wait:
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T postgres pg_isready -U taskq -d taskq >/dev/null 2>&1; then exit 0; fi; \
		printf 'ожидание PostgreSQL... (%s/30)\n' "$$i"; sleep 1; \
	done; \
	echo "PostgreSQL не стала доступна" >&2; exit 1

# Остановить тестовую PostgreSQL.
down:
	$(COMPOSE) down

# Интеграционные тесты (нужна доступная PostgreSQL; иначе тесты пропускаются).
test:
	go test -count=1 ./...

# Тесты с race detector.
test-race:
	go test -race -count=1 ./...

# Поднять PostgreSQL и прогнать тесты, оставив её запущенной для повторных прогонов.
test-local: up
	go test -count=1 ./...

# Самодостаточный прогон: up, тесты, down.
test-docker: up
	@echo "=== тесты (PostgreSQL) ==="
	@go test -count=1 -v ./...
	@$(MAKE) --no-print-directory down

# Статический анализ.
vet:
	go vet ./...

# Линтер (требует установленного staticcheck).
lint: vet
	staticcheck ./...

# Форматирование кода.
fmt:
	gofmt -w .

# Обновление зависимостей.
tidy:
	go mod tidy

# Сборка всех пакетов.
build:
	go build ./...

# Очистка: остановить PostgreSQL и сбросить кэш тестов.
clean: down
	go clean -testcache
