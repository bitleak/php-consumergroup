<?php

include 'consumer.php';

//signal processor
function sig_handler($signo) {      
    switch ($signo) {
        case SIGHUP:
        case SIGQUIT:
        case SIGTERM:
        case SIGINT:
            Consumer::stop();
            break;
        default:
    }
}
    
//callback function for process message
function echo_message($msg) {
    echo "$msg->payload ", "$msg->offset\n";
    usleep(200000);
    //simulate processing time
}

//callback function for handle error
function handleError($msg) {
    printf("topic: %s, partition: %d, error: %s\n",
        $msg->topic_name, $msg->partition, $msg->errstr());
}

//ticks
declare(ticks=1);

//set config
$zkAddress = "localhost:2181";
$topic = "php-test";
$groupId = "group-test-1";
$maxMessage = 10;

$consumer = New Consumer($zkAddress);
$consumer->setGroupId($groupId);
$consumer->setTopic($topic);
$consumer->setMaxMessage($maxMessage);
$consumer->setClientId("test");
$consumer->setOffsetAutoReset(Consumer::smallest);
$consumer->setErrHandler("handleError");

//install signal processor
pcntl_signal(SIGHUP, "sig_handler");
pcntl_signal(SIGINT, "sig_handler");
pcntl_signal(SIGQUIT, "sig_handler");
pcntl_signal(SIGTERM, "sig_handler");

//start to consume message
try {
    $consumer->start("echo_message");
}
catch(Exception $e) {
    printf("error: %s\n", $e->getMessage());
}
