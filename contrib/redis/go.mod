module github.com/denisdubovitskiy/taskq/contrib/redis

go 1.26.2

require (
	github.com/denisdubovitskiy/taskq v0.0.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/denisdubovitskiy/taskq => ../..
