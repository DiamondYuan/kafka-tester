FROM golang as build

COPY . /go/src/github.com/DiamondYuan/kafka-tester

ENV CGO_ENABLED=0

RUN curl https://glide.sh/get | sh

RUN cd /go/src/github.com/DiamondYuan/kafka-tester && \
    glide install && \
	go build

FROM alpine

COPY --from=build /go/src/github.com/DiamondYuan/kafka-tester/kafka-tester /usr/bin/

WORKDIR /

ENTRYPOINT ["kafka-tester"]
