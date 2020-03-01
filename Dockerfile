FROM golang:1.13.5 as build

RUN mkdir /go/src/app

RUN go get -u github.com/golang/dep/cmd/dep

ADD ./*.go /go/src/app/
COPY ./Gopkg.toml /go/src/app

WORKDIR /go/src/app

RUN dep ensure && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo -ldflags '-extldflags "-static"' -o app

FROM busybox:1.28
COPY --from=build /go/src/app /
VOLUME /certs
VOLUME /queries
EXPOSE 9440
ENTRYPOINT [ "/app" ]