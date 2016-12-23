<?php

require "../zkUtils.php";
class ZkUtilsTest extends PHPUnit_Framework_TestCase {

    protected static $zk_Utils;
    const TEST_PATH_PREFIX = "/php-zkUtils";
    const TEST_NO_EXITS = "/php-zkUtils/no-exists";
    const TEST_PATH = "/php-zkUtils/test";
    const TEST_GET_CHILDREN = "/php-zkUtils/test-get-children";
    const TEST_VALUE = "test_value";
    const TEST_TOPIC = "test_topic";
    const TEST_GROUPID = "test_groupid"; 

    public static function setUpBeforeClass() {
        self::$zk_Utils = new zkUtils("localhost:2188");
    }

    public static function tearDownAfterClass() {
        self::$zk_Utils->delete(self::TEST_PATH_PREFIX);
    }

    public function testSet() {
        $result = self::$zk_Utils->set(self::TEST_PATH, self::TEST_VALUE);
        $this->assertTrue($result);
    }

    /*
    * @depends testSet
    */
    public function testGet() {
        $result = self::$zk_Utils->get(self::TEST_PATH); 
        $this->assertEquals(self::TEST_VALUE, $result);
    }

    public function testDeleteEmpty() {
        $this->assertTrue(self::$zk_Utils->delete(self::TEST_NO_EXITS));
    }

    /*
    * @depends testSet
     */
    public function testDelete() {
        $this->assertTrue(self::$zk_Utils->delete(self::TEST_PATH));
    }

    /*
     * @depends testDelete
     */
    public function testSetEphemeral() {
        $this->assertTrue(self::$zk_Utils->setEphemeral(self::TEST_PATH, self::TEST_VALUE));
        $this->assertFalse(self::$zk_Utils->setEphemeral(self::TEST_PATH, self::TEST_VALUE));
        self::$zk_Utils->delete(self::TEST_PATH);
    }

    /* 
     * @depends testSet
     */
    public function testGetChildrenEmpty() {
        self::$zk_Utils->set(self::TEST_GET_CHILDREN,'');
        $result = self::$zk_Utils->getChildren(self::TEST_GET_CHILDREN); 
        $this->assertEmpty($result);
    }
    
    /*
     * @depends testSet
     * @depends testDelete
     */
    public function testGetChildren() {
        $count = 10;

        for($i = 0; $i < $count; ++$i) {
            self::$zk_Utils->set(self::TEST_GET_CHILDREN ."/$i", $i);
        }
        $result = self::$zk_Utils->getChildren(self::TEST_GET_CHILDREN );
        $this->assertCount($count, $result);

        for($i = 0; $i < $count; ++$i) {
            self::$zk_Utils->delete(self::TEST_GET_CHILDREN."/$i", $i);
        }
        self::$zk_Utils->delete(self::TEST_GET_CHILDREN);
    }

    /*
     *  @depends testSet
     *  @depends testGet
     *  @depends testDelete
     */
    public function testCommitAndGetOffset() {
        $offset = rand();
        $partition = 0;

        $this->assertEquals(0,self::$zk_Utils->getOffset(self::TEST_TOPIC, self::TEST_GROUPID, $partition)); 

        $result = self::$zk_Utils->commitOffset(self::TEST_TOPIC, self::TEST_GROUPID, $partition, $offset);     
        $this->assertTrue($result);

        $result = self::$zk_Utils->getOffset(self::TEST_TOPIC, self::TEST_GROUPID, $partition);
        $this->assertEquals($offset, $result);
        $path = "/consumers/".self::TEST_GROUPID."/offsets/".self::TEST_TOPIC."/$partition";
        self::$zk_Utils->delete($path);
        // TODO: 递归删除路径
    }

}
