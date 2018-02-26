# kafka-tester

A same app to test kafka

## How to use

### build with Docker
``` 
git clone https://github.com/DiamondYuan/kafka-tester.git

cd kafka-tester

docker  build -t kafka-tester:1.0 .
```

### run with Docker

If your kafka address is 192.168.10.67:9092

```$xslt
docker run kafka-tester:1.0 -address=192.168.10.67:9092 -topic=test
```

