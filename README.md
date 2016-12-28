# php-kafka-consumer

php-kafka-consumer is a kafka consumer library with group and rebalance supports.

[Chinese Doc](./README.zh-CN.md)

## Requirements

* Apache Kafka 0.7.x, 0.8.x, 0.9.x, 0.10.x

## Dependencies

* [php-zookeeper](https://github.com/php-zookeeper/php-zookeeper)
* [php_rdkafka](https://github.com/arnaud-lb/php-rdkafka/releases/tag/1.0.0) (1.0.0 is recommended)
* [librdkafka](https://github.com/edenhill/librdkafka/releases/tag/0.9.1) (0.9.1 is recommended)

## Performance

`78,000+` messages/s for single process

more detail  [benchmark](#benchmark)

## Example

```
<?php 
include 'consumer.php';

function call_back_func($msg) {
    echo "$msg->payload\n";
}

function handle_error_call_back($msg) {
    echo $msg->errstr();
}

$consumer = New Consumer("localhost:2181");
$consumer->setGroupId("group-test");
$consumer->setTopic("topic-test");
$consumer->setOffsetAutoReset(Consumer::smallest);
$consumer->setErrHandler("handle_error_call_back");

try {
    $consumer->start("echo_message");
}
catch(Exception $e) {
    printf("error: %s\n", $e->getMessage());
}
```

see [example.php](./example.php)

## Consumer Options

##### Consumer::setMaxMessage()

Number, defaults to `32`

If partitions > 1, it forces consumers to switch to other partitons when max message is reached, or other partitons will be starved

##### Consumer::setCommitInterval()

Millisecond, defaults to `500ms`

Offset auto commit interval.

##### Consumer::setWatchInterval()

Millisecond, defaults to `10,000 ms`

Time interval to check rebalance. Rebalance is triggered when the number of partition or consumer changes.

##### Consumer::setConsumeTimeout()

Millisecond, default is `1,000 ms`

Kafka request timeout.

##### Consumer::setClientId()

String, defaults to `"default"`

Client id is used to identify consumers. 

##### Consumer::setOffsetAutoReset()

`smallest|largest`, defaults to `smallest`

Consumer can choose whether to fetch the oldest or the lastest message when offset isn't present in zookeeper or is out of range.

##### Consumer::setConf()
Attribute and value are passed in

We can use this function to modify the librdkafka configuration。

more detail about [librdkafka configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). 

## Exception

* Recoverable exceptption (e.g. request timeout), error handler will be called, and you can log error messages.
* Unrecoverable exception (e.g. kafka/zookeeper is broken), exceptions will be thrown, and you should log message and stop the consumer.

## Benchmark

|Type|Parmeter|
|---|---|
|CPU|Intel(R) Xeon(R) CPU E5-2420 0 @ 1.90GHz|
|CPU Core|24|
|Memory|16G|
|Disk|SSD|
|Network|1000Mbit/s|
|Os|CentOS release 6.7|

Benchmark is measured by produring `20,000,000` messages at single partition, and calculate the time it takes to consume those messages.

QPS is `78,000` messages/s when process cpu utility is `100%`.
