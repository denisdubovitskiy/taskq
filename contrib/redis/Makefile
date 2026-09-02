# taskq contrib/redis — цели разработки и интеграционных тестов.
#
# Интеграционные тесты требуют Redis. По умолчанию поднимаем свой через
# docker compose (порт 6380, см. docker-compose.yml); адрес переопределяется
# переменной окружения TASKQ_TEST_REDIS_ADDR (или REDIS_ADDR ниже).
#
# Быстрый старт:
#   make test-docker   # up + тесты + down — самодостаточно, ничего не оставляет

.DEFAULT_GOAL := help

COMPOSE := docker compose

# Адрес тестового Redis. Если TASKQ_TEST_REDIS_ADDR уже задан в окружении —
# уважаем его; иначе дефолт совпадает с маппингом docker-compose.yml.
ifndef TASKQ_TEST_REDIS_ADDR
REDIS_ADDR := localhost:6380
else
REDIS_ADDR := $(TASKQ_TEST_REDIS_ADDR)
endif
export TASKQ_TEST_REDIS_ADDR = $(REDIS_ADDR)

.PHONY: help up wait down test test-race test-local test-docker vet lint fmt tidy build clean

# Справка (по умолчанию)
help:
	@echo "taskq contrib/redis"
	@echo ""
	@echo "Цели:"
	@echo "  make test         — интеграционные тесты (нужен Redis, дефолт $(REDIS_ADDR))"
	@echo "  make test-race    — тесты с race detector"
	@echo "  make up           — поднять Redis (docker compose, ждать готовности)"
	@echo "  make wait         — дождаться готовности Redis"
	@echo "  make down         — остановить Redis (docker compose)"
	@echo "  make test-local   — up + тесты (Redis остается запущенным)"
	@echo "  make test-docker  — up + тесты + down (самодостаточно)"
	@echo "  make vet          — go vet"
	@echo "  make lint         — go vet + staticcheck (нужен staticcheck)"
	@echo "  make fmt          — gofmt -w ."
	@echo "  make tidy         — go mod tidy"
	@echo "  make build        — собрать пакеты"
	@echo "  make clean        — down + go clean -testcache"

# Поднять тестовый Redis и дождаться, пока он станет healthy.
up:
	$(COMPOSE) up -d --wait

# Дождаться готовности Redis (периодический ping, до ~30 c).
wait:
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T redis redis-cli ping >/dev/null 2>&1; then exit 0; fi; \
		printf 'ожидание Redis... (%s/30)\n' "$$i"; sleep 1; \
	done; \
	echo "Redis не стал доступен" >&2; exit 1

# Остановить тестовый Redis.
down:
	$(COMPOSE) down

# Интеграционные тесты (нужен доступный Redis; иначе тесты пропускаются).
test:
	go test -count=1 ./...

# Тесты с race detector.
test-race:
	go test -race -count=1 ./...

# Поднять Redis и прогнать тесты, оставив его запущенным для повторных прогонов.
test-local: up
	go test -count=1 ./...

# Самодостаточный прогон: up, тесты, down.
test-docker: up
	@echo "=== тесты (Redis $(REDIS_ADDR)) ==="
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

# Очистка: остановить Redis и сбросить кэш тестов.
clean: down
	go clean -testcache
