<?php
/**
 * Utils used by kafka client to communicate with zookeeper 
 */
class zkUtils {
    const consumer_dir = '/consumers';
    const broker_topics_dir = '/brokers/topics';
    const brokers_dir = '/brokers/ids';
    const EPHEMERAL = 1;
    const SEQUENCE = 2;

    /**
     * default acl whening creating a zonde in zookeeper
     */
    static $acl = array(
        array('perms' => 0x1f, 'scheme' => 'world', 'id' => 'anyone')
    );

    private $zookeeper;

    public function __construct($address, $sessionTimeout = 30000) {
        $this->zookeeper = new Zookeeper($address, null, $sessionTimeout);
    }

    static function filterEmpty($e) {
        return $e !=false || $e === "0" || $e === 0;
    }

    /**
     * make persistent path recursive
     *
     * @var String  $path           path
     * @var String  $value          value
     * @var Int     $flags          flags to indicates which type of node to create
     * $return true or false
     */
    private function makeNode($path, $value, $flags=null) {
        return $this->zookeeper->create($path, $value, self::$acl, $flags) != null ? true : false;
    }


    /**
     * make persistent path recursive
     *
     * @var String  $path           path
     * @var String  $value          value
     */
    private function makePath($path, $value = '') {
        $parts = explode('/', $path);
        $parts = array_filter($parts, 'zkUtils::filterEmpty');
        $subpath = '';
        while (count($parts) > 1) {
            $subpath .= '/' . array_shift($parts);
            if (!$this->zookeeper->exists($subpath)) {
                $this->makeNode($subpath, $value); 
            }
        }
    }

    /**
     * set a persistent node
     *
     * @var String  $path           path
     * @var String  $value          value
     * $return true or false
     */
    public function set($path, $value) {
        if (!$this->zookeeper->exists($path)) {
            $this->makePath($path);
            return $this->makeNode($path, $value);
        } 
        return $this->zookeeper->set($path, $value);
    }

    /**
     * set an ephemeral node
     *
     * @var String  $path           path
     * @var String  $value          value
     * $return true or false
     */
    public function setEphemeral($path, $value) {
        if (!$this->zookeeper->exists($path)) {
            $this->makePath($path);
            return $this->makeNode($path, $value, self::EPHEMERAL);
        } 
        return false;
    }

    /**
     * get the node on the path
     *
     * @var String $path             path
     * $return the node name
     */
    public function get($path) {
        if (!$this->zookeeper->exists($path)) {
            return null;
        }
        return $this->zookeeper->get($path);
    }

    /**
     * delete the path
     *
     * @var String  $path           path
     * $return true or false
     */
    public function delete($path) {
        if (!$this->zookeeper->exists($path)) {
            return true;
        } 
        return $this->zookeeper->delete($path);
    }

    /**
     * get the child nodes under the path
     *
     * @var String  $path           path
     * $return an array of strings about nodes' name
     */
    public function getChildren($path) {
        if (strlen($path) > 1 && preg_match('@/$@', $path)) {
            $path = substr($path, 0, -1);
        }
        return $this->zookeeper->getChildren($path);
    }        

    /**
     * commit offset to zookeeper
     *
     * @var String  $topic          topic
     * @var String  $groupId        consumer group Id
     * @var Int     $partition      partition
     * @var Int     $offset         consumed msg offset in one partition
     * $return true or false
     */
    public function commitOffset($topic, $groupId, $partition, $offset) {
        $path = self::consumer_dir."/$groupId/offsets/$topic/$partition";
        return $this->set($path, $offset);
    }

    /**
     * Get a partition's message offset from zookeeper
     *
     * @var String  $topic          topic
     * @var String  $groupId        consumer group Id
     * @var Int     $partition      partition
     * $return Int offset
     */
    public function getOffset($topic, $groupId, $partition) {
        $path = self::consumer_dir."/$groupId/offsets/$topic/$partition";
        if($this->zookeeper->exists($path)) {
            return $this->zookeeper->get($path);
        } else {
            //if offset not found, return -1 and consumer will reset offset base on autoReset
            return -1;
        }
    }

    /**
     * register a consumer for the group
     *
     * @var String  $topic          topic
     * @var String  $groupId        consumer group Id
     * @var String  $consumerId     consumer Id
     * $return ture or false
     */
    public function registerConsumer($topic, $groupId, $consumerId)  {
        $path = self::consumer_dir."/$groupId/ids/$consumerId";
        return $this->setEphemeral($path, ' ');
    }

    /**
     * delete a consumer for the group
     *
     * @var String  $topic          topic
     * @var String  $groupId        consumer group Id
     * @var String  $consumerId     consumer Id
     * $return ture or false
     */
    public function deleteConsumer($topic, $groupId, $consumerId) {
        $path = self::consumer_dir."/$groupId/ids/$consumerId";
        return $this->delete($path);
    }

    /**
     * get partition list for the topic
     *
     * @var String  $topic          topic
     * $return an array of strings about partition number
     */
    public function getPartitions($topic) {
        $path = self::broker_topics_dir."/$topic/partitions";
        return $this->getChildren($path);
    }

    /**
     * get consumer list for the group
     *
     * @var String  $groupId        consumer group Id
     * $return an array of strings about consumer name
     */
    public function getConsumers($groupId) {
        $path = self::consumer_dir."/$groupId/ids";
        return $this->getChildren($path);
    }

    /**
     * register the partition's owner
     *
     * @var String  $topic          topic
     * @var String  $groupId        consumer group Id
     * @var Int     $partition      partition
     * @var String  $consumerId     consumer Id
     * $return ture or false
     */
    public function registerOwner($topic, $groupId, $partition, $consumerId) {
        $path = self::consumer_dir."/$groupId/owners/$topic/$partition";
        return $this->setEphemeral($path, $consumerId);
    }

    /**
     * release the partition's owner
     *
     * @var String  $topic          topic
     * @var String  $groupId        consumer group Id
     * @var Int     $partition      partition
     * $return ture or false
     */
    public function releasePartitionOwnership($topic, $groupId, $partition) {
        $path = self::consumer_dir."/$groupId/owners/$topic/$partition";
        if ($this->zookeeper->exists($path)) {
            return $this->delete($path);
        }  
        return true;
    }

    /**
     * get broker list for this cluster
     *
     * $return a string about broker list, elements that including ip adress and prot are splitted by ',' .
     */
    public function getBrokerList() {
        $path = self::brokers_dir;
        $brokers = $this->getChildren($path);
        $brokerList = "";
        foreach($brokers as $broker) {
            $info = $this->get(self::brokers_dir."/$broker");
            if ($info != null && $info !=false) {
                $json_decode = json_decode($info, true);
                if ($brokerList !== '') {
                    $brokerList .= ",";
                }
                $brokerList .= $json_decode['host'] . ":" .  $json_decode['port'];
            }
        }
        return $brokerList;
    }
}
