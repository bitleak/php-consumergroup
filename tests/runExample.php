<?php

include '../consumer.php';

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

function echo_message($msg) {
    $file = fopen("consumerTestTempFile", "a+");
    fwrite($file, $GLOBALS['consumerId']." $msg->partition $msg->offset\n");
    usleep(20*1000);
    fclose($file);
}

declare(ticks=1);

$zkAddress = "localhost:2181";
$topic = "php-test";
$groupId = "group-test-1";
$maxMessage = 1;

$consumer = New Consumer($zkAddress);
$consumer->setGroupId($groupId);
$consumer->setTopic($topic);
$consumer->setMaxMessage($maxMessage);
$consumer->setClientId("test");
$consumer->setConsumeTimeout(1000);
$consumer->setWatchInterval(10);

pcntl_signal(SIGHUP, "sig_handler");
pcntl_signal(SIGINT, "sig_handler");
pcntl_signal(SIGQUIT, "sig_handler");
pcntl_signal(SIGTERM, "sig_handler");

$GLOBALS['consumerId'] = $argv[1];
$consumer->start("echo_message");
