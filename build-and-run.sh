cd src
rm go-mux-postgresql-gorm
go mod download
CGO_ENABLED=0 GOOS=linux go build -o ./go-mux-postgresql-gorm
cd ..
bash -c 'cd src && ./go-mux-postgresql-gorm'