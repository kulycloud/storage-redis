FROM golang:1.15.3-alpine AS builder

ADD storage-redis/go.mod storage-redis/go.sum /build/storage-redis/
ADD protocol/go.mod protocol/go.sum /build/protocol/
ADD common/go.mod common/go.sum /build/common/

ENV CGO_ENABLED=0

WORKDIR /build/storage-redis
RUN go mod download

COPY storage-redis/ /build/storage-redis/
COPY protocol/ /build/protocol
COPY common/ /build/common
RUN go build -o /build/kuly .

FROM scratch

COPY --from=builder /build/kuly /

CMD ["/kuly"]
