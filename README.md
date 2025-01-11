# Golang Pub/Sub (Worker)
Golang app that act as a worker. Subscribe to topics to handle several jobs

## How to run?
1. Export credential path for IAM service account
```shell
export GOOGLE_APPLICATION_CREDENTIALS=/Path/to/account/service/key/json
```
2. execute
```shell
`go run main.go`
```