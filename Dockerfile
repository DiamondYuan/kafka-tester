FROM golang
MAINTAINER FandiYuan  <georgeyuan@diamondyuan.com>

RUN go get github.com/Shopify/sarama

ADD main.go /kafka-test-temp/

RUN cd /kafka-test-temp && \
	go build && \
	mv kafka-test-temp /kafka-test && \
	rm -rf /kafka-test-temp

CMD ["/kafka-test"]
