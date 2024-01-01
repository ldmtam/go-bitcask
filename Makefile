run-test:
	go test -v ./...

run-coverage:
	go test ./... -coverprofile=cover.out
	go tool cover -html=cover.out