## php-kafka-consumer
### 简介
主要是对php_rdkafka的consumer的api进行了一层封装，增加了原程序中所没有的与zookeeper交互的功能。在此基础上实现了rebalance功能以及group功能。
producer及其他相关的内容可以参考[php_rdkafka](https://github.com/arnaud-lb/php-rdkafka) 中的相关文档。
经过简单的压力测试，单个进程的消费能力能达到每秒钟7.8W条，压测详细内容见[压力测试](#压力测试)。

### 依赖
[php_zookeeper](https://github.com/php-zookeeper/php-zookeeper)

[php_rdkafka](https://github.com/arnaud-lb/php-rdkafka/releases/tag/1.0.0) （建议使用1.0.0版本）

[librdkafka](https://github.com/edenhill/librdkafka/releases/tag/0.9.1)（建议使用0.9.1版本）

### 使用
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

更详细的例子见[example](./example.php)。

### 关于异常处理
* 对于一些可恢复的异常，比如获取消息超时之类的，提供回调函数，建议的做法是把这些异常都记录到日志中，方便到时排查问题。回调函数执行完毕后会继续进行消费。
* 对于一些不可恢复的异常，诸如kafka挂掉了，zookeeper不可用之类的，抛出到最外层，由使用者决定该如何操作。建议的做法是记录日志之后退出消费，等到异常问题排查之后，再重新开始消费。

### 配置参数
目前支持的配置项较少，以后会根据需求再进行增加。
#### Consumer::setMaxMessage()
一个整数，表示当一个consumer轮询消费多个partition的时候，每个partition最多能消费多少条信息。用来避免只消费一个partition，而忽略了其他的partition。默认值是1000。

#### Consumer::setCommitInterval()
一个整数，超过这个时间，consumer就自动提交一次每个partition的offset到zookeeper。减少因consumer进程突然死亡而造成的重复消费的消息的条数。默认值是500ms。

#### Consumer::setWatchInterval()
一个整数，超过这个时间，consumer就会开始检查是否有consumer或者partition的变动。如果有变化的话就会自行rebalance。默认值是10000ms。

#### Consumer::setConsumeTimeout()
一个整数，向server发送请求获取的超时时间。默认是1000ms。

#### Consumer::setClientId()
一个字符串，加在consumerId的最后，用来表示是当前consumer是属于哪个应用的。

#### Consumer::setOffsetAutoReset()
可选值为smallest和largest。如果zookeeper中没有offset或者offset超出了kafka中消息的范围，将会自动选择从最小的offset开始消费还是从最新的offset开始消费。如果选择smallest，将会从kafka中保存的消息中最旧的一条开始消费。如果选择largest，将会从最新的一条消息开始消费，即consumer启动后写入到kafka的第一条开始。

#### Consumer::setConf()
通过这个函数可以配置librdkafk的配置项，传入两个参数，分别是attribute及value。详细的配置项列表可以见[librdkafka的官方文档](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)。

### 注意事项
1) 虽然consumer在zookeeper上注册都是临时节点，超过一定时间没有心跳包发送将会自动掉线，也能触发rebalance。但是为了能使在主动关闭的情况下做到, 其他consumer更快察觉到有consumer关闭，更早进行rebalance
    所以建议使用时，安装信号处理器，在收到进程退出的信号时调用Consumer::stop()，使consumer能正常退出, 关闭前提交offset，尽量避免重复消费。。详细例子见[example](./example.php)。

2) consumer在运行中如果连续报严重错误一定次数(当前写死为256次)之后会抛出一个"kafka server is not available."的Exception。严重错误包括kafka server挂掉、server端所在机器上的系统损坏等。如果正常消费会将计数清零。因为consumer内有缓存队列，每个partition 1M的大小，所以必须等这部分消耗完之后再报错连续一定次数才会抛出这个Exception。

### 压力测试
当前进行了简单的压力测试，创建了一个单partition的topic，开启一个客户端进程对客户端性能进行压测。配置项全部使用了默认配置。瓶颈点出现在客户端的cpu占用率上，达到了100%。读取2000W条数据，最后计算出来，每秒平均消费大概7.8W条数据。

#### 测试时客户端的机器配置
|类型|参数|
|---|---|
|CPU|Intel(R) Xeon(R) CPU E5-2420 0 @ 1.90GHz|
|核数|24|
|内存|16G|
|硬盘|SSD|
|网卡|千兆网卡|
|操作系统|CentOS release 6.7|


