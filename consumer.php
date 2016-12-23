<?php
include "zkUtils.php";

$__serverDownTimes = 0;

class Consumer {
    const smallest = 'smallest';
    const largest = 'largest';

    private $groupId;
    private $topic;
    private $maxMessage;
    private $commitInterval;
    private $watchInterval;
    private $consumeTimeout;
    private $conf;

    private $zkUtils;
    private $rkTopic;
    private $consumerIdPrefix;
    private $consumerId;
    private $partitions=array();
    private $currentPartitions=array();
    private $consumedPartitions=array();
    private $consumers=array();
    private $offsets=array();
    private $prevOffsets=array();
    private $rk;
    private $lastCommitTime;
    private $lastWatchTime;
    private $errHandler;
    private $eofHandler;
    private $offsetAutoReset;
    private static $running = true;

    /**
     * constructor
     *
     * @var String  $zkAdress           list of host:port values (e.g. "host1:2181,host2:2181")
     * @var Int     $sessionTimeout     zookeeper session timeout
     */
    public function __construct($zkAddress, $sessionTimeout = 30000) {
        $this->zkUtils = new zkUtils($zkAddress, $sessionTimeout);
        $this->topic = null;
        $this->maxMessage = 32;
        $this->consumerIdPrefix = getHostByName(getHostName()) . "-" . getmypid() . "-" . microtime(true);
        $this->consumerId = $this->consumerIdPrefix . "default";
        $this->commitInterval = 500;
        $this->watchInterval = 10000;
        $this->consumeTimeout = 1000;
        $this->errHandler = function ($msg) {
            printf("partition: %d , err: %s \n", $msg->partition, $msg->errstr());
        };

        $this->offsetAutoReset = self::smallest;

        $this->conf = new Rdkafka\Conf();
        $this->conf->set('broker.version.fallback', '0.8.2');
        $this->conf->set('queued.max.messages.kbytes', 1024);
        $this->conf->set('topic.metadata.refresh.interval.ms', 60000);
        $this->conf->set('fetch.message.max.bytes', 1048576);
        $this->conf->setErrorCb(function ($kafka, $err, $reason) {
            printf("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason);
            global $__serverDownTimes; 
            $__serverDownTimes += 1;
            if ($__serverDownTimes > 256) {
                throw new Exception ("kafka server is not available.");
            }
        });
    }

    /**
     * Modify the librdkafka configuration
     *
     * @var String  $attribute      config attribute
     * @var         $value          config value
     */
    public function setConf($attribute, $value) {
        $this->conf->set($attribute, $value);
    }

    /**
     * Set consumer group Id, this value must be set
     *
     * @var String  $groupId        consumer group Id
     */
    public function setGroupId($groupId) {
        $this->groupId = $groupId;
    }

    /**
     * Set topic, this value must be set
     *
     * @var String  $topic          topic
     */
    public function setTopic($topic) { 
        $this->topic = $topic;
    }

    /**
     * If partitions > 1, it forces consumers to switch to other
     * partitons when max message is reached, or other partitons
     * will be starved.
     *
     * @var Int     $maxMessage     max message number, defaults to 32
     */
    public function setMaxMessage($maxMessage) { 
        $this->maxMessage = $maxMessage;
    }

    /**
     * Set offset auto commit interval.
     *
     * @var Ind     $commitInterval     the unit is milliseconds, defaults to 500
     */
    public function setCommitInterval($commitInterval) {
        $this->commitInterval = $commitInterval;
    }

    /**
     * Set time interval to check rebalance. Rebalance is triggered
     * when the number of partition or consumer changes.
     *
     * @var Int     $watchInterval      the unit is milliseconds, defaults to 10000
     */
    public function setWatchInterval($watchInterval) {
        $this->watchInterval = $watchInterval;
    }

    /**
     * Set kafka request timeout.
     *
     * @var Int     $consumeTimeout     the unit is milliseconds, defaults to 1000
     */
    public function setConsumeTimeout($consumeTimeout) {
        $this->consumeTimeout = $consumeTimeout;
    }

    /**
     * Set client id is used to identify consumers
     *
     * @var String  $clientId       client id, defaults to "default"
     */
    public function setClientId($clientId) {
        $this->consumerId = $this->consumerIdPrefix . '-' . $clientId;
    }

    /**
     * Set a callback function is used to handle error
     *
     * @var Function    $errorHandler   error handle functioin
     */
    public function setErrHandler($errHandler) {
        $this->errHandler = $errHandler;
    }

    /**
     * Set a callback function is used to handle when consumer offset
     * reach the end of partition
     *
     * @var Function    $eofHandler     eof handle function
     */
    public function setEofHandler($eofHandler) {
        $this->eofHandler = $eofHandler;
    }

    /**
     * Set offset auto reset rule. Consumer can choose whether to fetch
     * the oldest or the lastest message when offset isn't present in
     * zookeeper or is out of range.
     *
     * @var Int     $autoReset      smallest or largest, defaults to samllest
     */
    public function setOffsetAutoReset($autoReset) {
        if ($autoReset === self::smallest || $autoReset === self::largest) {
            $this->offsetAutoReset = $autoReset;
        } else {
            throw new Exception ("invalid offset auto reset argument: you should set
                smallest or largest");
        }
    }

    //get current time
    private function getTime() {
        return microtime(true) * 1000;
    }

    private function rebalance() {
        $cnt = count($this->consumers);
        $this->consumedPartitions = array();
        for ($i=0; $i<count($this->partitions); $i++) {
            if ($this->consumers[$i % $cnt] === $this->consumerId) 
                array_push($this->consumedPartitions, $this->partitions[$i]);
        }
    }

    private function checkOwner() {
        $curr = $this->currentPartitions;
        $csm = $this->consumedPartitions;
        if ($curr === $csm) return; 

        //release unnecessary partition after rebalance
        $diff = array_values(array_diff($curr, $csm));
        foreach($diff as $partition) {
            if (!$this->zkUtils->releasePartitionOwnership($this->topic, $this->groupId, 
                $partition)) {
                    throw new Exception ("release partition ownership failed");
                } else {
                    array_splice($curr, array_search($partition, $curr), 1);
                    $this->zkUtils->commitOffset($this->topic, $this->groupId, 
                        $partition, $this->offsets[$partition]);
                    $this->rkTopic->consumeStop($partition);
                }
        }

        //register owner for new partitions after rebalance
        $diff = array_values(array_diff($csm, $curr));
        foreach($diff as $partition) {
            if ($this->zkUtils->registerOwner($this->topic, $this->groupId, 
                $partition, $this->consumerId)) {
                    array_push($curr, $partition);
                    $offset = $this->zkUtils->getOffset($this->topic, $this->groupId, $partition);
                    if ($offset < 0) {
                        $this->rkTopic->consumeStart($partition, $this->offsetAutoReset === self::smallest ? -2 : -1 );
                    } else {
                        $this->rkTopic->consumeStart($partition, $offset);
                    }
                    $this->offsets[$partition] = $offset;
                    $this->prevOffsets[$partition] = $offset;
                }
        }
        sort($curr);
        $this->currentPartitions = $curr; 
    }

    private function needRebalance() {
        if (empty($this->currentPartitions)) {
            usleep(100000);
        } else if ($this->getTime() - $this->lastWatchTime < $this->watchInterval) {
            return false;
        }
        $needRebalance = false;
        $this->lastWatchTime = $this->getTime();

        //trigger rebalance when partition or consumer number has changed
        $partitions = $this->zkUtils->getPartitions($this->topic);
        sort($partitions);
        if ($partitions !== $this->partitions) {
            $this->partitions = $partitions;
            $needRebalance= true;
        }

        $consumers = $this->zkUtils->getConsumers($this->groupId, $this->topic);
        sort($consumers);
        if ($consumers !== $this->consumers) {
            $this->consumers = $consumers;
            $needRebalance = true;
        }

        return $needRebalance;
    }

    private function commitOffset($partitions) {
        foreach($partitions as $partition) {
            if ($this->offsets[$partition] != $this->prevOffsets[$partition]) {
                $this->zkUtils->commitOffset($this->topic, $this->groupId, 
                    $partition, $this->offsets[$partition]);
                $this->prevOffsets[$partition] = $this->offsets[$partition];
            }
        }
    }

    /**
     * start to process messages by this callback function
     *
     * @var function  $callback_func    callback function to process message
     */
    public function start($callback_func) {
        if (empty($this->groupId) || empty($this->topic)) {
            throw new Exception("please set groupId and topic to this consumer");
        }
        $this->rk = new Rdkafka\Consumer($this->conf);
        $brokerList = $this->zkUtils->getBrokerList();
        if ($brokerList == "") {
            throw new Exception ("broker list is empty!");
        }
        $this->rk->addBrokers($brokerList);

        $topicConf = new Rdkafka\TopicConf();
        $topicConf->set('auto.offset.reset', $this->offsetAutoReset);
        $this->rkTopic = $this->rk->newTopic($this->topic, $topicConf);

        $this->lastCommitTime = $this->getTime();
        if (!$this->zkUtils->registerConsumer($this->topic, $this->groupId, $this->consumerId)) {
            throw new Exception("start failed");
            exit;
        }

        $this->lastWatchTime = 0;
        while (self::$running) {
            $this->consume($callback_func);
        }
        $this->shutdown();
    }

    private function shutdown() { 
        $this->commitOffset($this->currentPartitions);
        //release partitions, and commit offsets;
        foreach($this->currentPartitions as $partition) {
            if (!$this->zkUtils->releasePartitionOwnership($this->topic, $this->groupId, 
                $partition)) {
                    throw new Exception ("release partition ownership failed");
                } else {
                    $this->rkTopic->consumeStop($partition);
                }
        }
        $this->zkUtils->deleteConsumer($this->topic, $this->groupId, $this->consumerId);
        $this->currentPartitions = array();
    }

    /**
     * stop consuming messages
     */
    public static function stop() {
        self::$running = false;
    }

    private function consume($callback_func) { 
        global $__serverDownTimes;
        if ($this->needRebalance()) {
            $this->rebalance();
        }
        $this->checkOwner();

        //consume message in partitions
        foreach ($this->currentPartitions as $partition) {
            $offset = $this->offsets[$partition];
            $cnt = 0;
            while ($cnt++ < $this->maxMessage) {
                $msg = $this->rkTopic->consume($partition, $this->consumeTimeout);
                $this->rk->poll(0);
                if ($msg !== null && $msg->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                    call_user_func($callback_func, $msg);
                    $offset = $msg->offset + 1;
                } 
                else if ($msg === null || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    if ($this->eofHandler != NULL) {
                        call_user_func($this->eofHandler, $msg);
                    }
                    break;
                } else if($msg->err === RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT || 
                    $msg->err === RD_KAFKA_RESP_ERR__FAIL || 
                    $msg->err === RD_KAFKA_RESP_ERR__TRANSPORT) {
                        call_user_func($this->errHandler, $msg);
                    } else {
                        throw new Exception($msg->errstr());
                    }

                if (!($msg === null || $msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)) {
                    $__serverDownTimes = 0;
                }
            }
            //commit offset to zookeeper when interval time is reached
            $this->offsets[$partition] = $offset;
            if ($this->getTime() - $this->lastCommitTime > $this->commitInterval) {
                $this->commitOffset($this->currentPartitions);
                $this->lastCommitTime = $this->getTime();
            }
        }       
    }
}
