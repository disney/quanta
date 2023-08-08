
FROM golang:1.20  

RUN apt update

WORKDIR /quanta

COPY go.sum ./
COPY go.mod ./

RUN go mod download
RUN go mod verify

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /usr/bin/quanta-node  quanta-node.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /usr/bin/quanta-proxy  ./quanta-proxy/main.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /usr/bin/sqlrunner  ./sqlrunner/driver.go

EXPOSE 4000

RUN mkdir ./data-dir
RUN mkdir ./data-dir/bitmap
