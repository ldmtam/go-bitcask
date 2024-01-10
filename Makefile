run-test:
	go test -coverprofile=profile.cov -v ./...
	go tool cover -func=profile.cov | grep total | awk '{print $3}' | tee coverage.log
	rm -rf coverage.log profile.cov

check-lint:
	golangci-lint run

run-actions:
	act --container-architecture linux/amd64