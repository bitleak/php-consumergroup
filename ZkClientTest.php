<?php

require "ZkClient.php";
class ZkClientTest extends PHPUnit_Framework_TestCase {

    protected $zk_client;
    const TEST_NO_EXITS = "/php-zkclient/no-exists";
    const TEST_PATH = "/php-zkclient/test";
    const TEST_VALUE = "test_value";
    const TEST_TOPIC = "test_topic";
    const TEST_GROUPID = "test_groupid"; 

    protected function setUp() {
        $this->zk_client = new ZkClient("localhost:2181");
    }

    protected function tearDown() {
    }

    public function testSet() {
        $result = $this->zk_client->set(self::TEST_PATH, self::TEST_VALUE);
        $this->assertTrue($result);
    }

    /*
    * @depends testSet
    */
    public function testGet() {
        $result = $this->zk_client->get(self::TEST_PATH); 
        $this->assertEquals(self::TEST_VALUE, $result);
    }

    public function testDeleteEmpty() {
        //$this->assertTrue($this->zk_client->delete(self::TEST_NO_EXITS));
    }

    /*
    * @depends testSet
    */
    public function testDelete() {
        $this->assertTrue($this->zk_client->delete(self::TEST_PATH));
    }

    public function testGetChildrenEmpty() {
        try {
            $result = $this->zk_client->getChildren(self::TEST_NO_EXITS); 
        } catch (Exception $e) {
        }
        $this->assertNull($result);
    }
    
    public function testGetChildren() {
        $count = 10;
        $path = "/php/test-getchildren";

        for($i = 0; $i < $count; ++$i) {
            $this->zk_client->set($path."/$i", $i);
        }
        $result = $this->zk_client->getChildren($path);
        $this->assertCount($count, $result);

        for($i = 0; $i < $count; ++$i) {
            $this->zk_client->delete($path."/$i", $i);
        }
        $this->zk_client->delete($path);
        $this->zk_client->delete("/php");
    }
    
    public function testCommitAndGetOffset() {
        $offset = 12345;
        $partition = 0;
        $result = $this->zk_client->commitOffset(self::TEST_TOPIC, self::TEST_GROUPID, $partition, $offset);     
        $this->assertTrue($result);

        $result = $this->zk_client->getOffset(self::TEST_TOPIC, self::TEST_GROUPID, $partition);
        $this->assertEquals($offset, $result);
        // TODO: 递归删除路径
    }
}
