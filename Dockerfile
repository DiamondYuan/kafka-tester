FROM golang
MAINTAINER FandiYuan  <georgeyuan@diamondyuan.com>

RUN go get github.com/Shopify/sarama && \
	go get github.com/bsm/sarama-cluster

ADD main.go /kafka-testr-temp/

RUN cd /kafka-testr-temp && \
	go build && \
	mv kafka-testr-temp /kafka-testr && \
	rm -rf /kafka-testr-temp

ENTRYPOINT ["/kafka-testr"]
