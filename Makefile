.PHONY: test test-race test-contrib vet lint fmt tidy build example clean docs docs-serve docs-deploy docs-deps examples-build docs-check tools ci-local ci-local-lint

# ── Документация (MkDocs + Material + Poetry) ─────────────────────
# Poetry-окружение создаёт venv в ./.venv (virtualenvs.in-project = true).
# `make docs` установит зависимости при первом запуске и соберёт сайт.
MKDOCS := $(CURDIR)/.venv/bin/mkdocs

# ── Инструменты разработки (ставятся в ./bin, не в систему) ───────
# Версии зафиксированы: локальный запуск и CI используют одинаковые.
BIN_DIR := $(CURDIR)/bin
GOLANGCI_LINT_VERSION := v2.11.4
ACT_VERSION := v0.2.89

# Установить все инструменты в ./bin.
tools: bin/golangci-lint bin/act

# golangci-lint — официальный install-скрипт кладёт бинарник в ./bin.
bin/golangci-lint:
	mkdir -p bin
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh \
		| sh -s -- -b $(BIN_DIR) $(GOLANGCI_LINT_VERSION)

# act — локальный запуск GitHub Actions через docker.
bin/act:
	mkdir -p bin
	GOBIN=$(BIN_DIR) go install github.com/nektos/act@$(ACT_VERSION)

docs:
	poetry install --no-root
	$(MKDOCS) build -s

docs-serve:
	poetry install --no-root
	$(MKDOCS) serve -a 0.0.0.0:8000

docs-deploy:
	poetry install --no-root
	$(MKDOCS) gh-deploy -m "docs: rebuild site"

# Явная установка/обновление зависимостей документации
docs-deps:
	poetry install --no-root

# Проверка примеров из документации (отдельный go-модуль)
examples-build:
	cd docs/examples && go build ./...

# Сопоставимость языковых версий документации: docs/en и docs/ru
# должны содержать одинаковый набор страниц.
docs-check:
	@test "$$(ls docs/en | sort)" = "$$(ls docs/ru | sort)" \
		|| { echo "docs/en and docs/ru must contain the same set of pages"; ls docs/en docs/ru; exit 1; }

# Запуск тестов
test:
	go test ./...

# Запуск тестов с race detector
test-race:
	go test -race ./...

# Интеграционные тесты contrib-модулей (требует docker compose)
# В каждом каталоге: docker compose up -d перед запуском
test-contrib:
	cd contrib/redis && go test -count=1 ./...
	cd contrib/postgresql && go test -count=1 ./...
	cd contrib/prometheus && go test -count=1 ./...
	cd contrib/otel && go test -count=1 ./...

# Статический анализ
vet:
	go vet ./...

# Линтер: golangci-lint из ./bin (устанавливается по необходимости).
# Конфигурация — .golangci.yml; покрывает все модули (ядро и contrib).
lint: bin/golangci-lint
	$(BIN_DIR)/golangci-lint run ./...
	cd contrib/redis && ../../bin/golangci-lint run ./...
	cd contrib/postgresql && ../../bin/golangci-lint run ./...
	cd contrib/prometheus && ../../bin/golangci-lint run ./...
	cd contrib/otel && ../../bin/golangci-lint run ./...

# Локальный запуск GitHub Actions (.github/workflows) через act в docker.
# Аналог пуша в репозиторий: lint + тесты ядра + интеграционные тесты
# (Redis/PostgreSQL поднимаются docker compose на хосте docker).
# Сокет демона для job-контейнеров задан в .actrc (путь внутри VM докера),
# а DOCKER_HOST — endpoint текущего docker context (например, Lima VM на macOS).
# GITHUB_TOKEN обнуляется: act иначе подставляет его при клонировании actions,
# и невалидный токен ломает анонимный clone публичных репозиториев.
DOCKER_HOST_FROM_CONTEXT = $$(docker context inspect --format '{{json .Endpoints}}' | sed -E 's/.*"Host":"([^"]+)".*/\1/')

ci-local: bin/act
	GITHUB_TOKEN= DOCKER_HOST=$(DOCKER_HOST_FROM_CONTEXT) \
		$(BIN_DIR)/act push

# То же, но только линт-джоба (быстрая проверка перед коммитом).
ci-local-lint: bin/act
	GITHUB_TOKEN= DOCKER_HOST=$(DOCKER_HOST_FROM_CONTEXT) \
		$(BIN_DIR)/act push -j lint

# Форматирование кода (.gocache/.venv/site исключены — там нет исходников проекта)
GOFMT_FILES := $(shell find . -name '*.go' \
	-not -path './.gocache/*' -not -path './.venv*' \
	-not -path './.venv-docs/*' -not -path './site/*' -not -path './bin/*' 2>/dev/null)

fmt:
	gofmt -w $(GOFMT_FILES)

# Обновление зависимостей
tidy:
	go mod tidy

# Сборка всех пакетов
build:
	go build ./...

# Запуск приложения-примера
example:
	go run ./example

# Очистка кэша
clean:
	go clean -testcache
