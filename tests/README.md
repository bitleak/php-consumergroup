## 说明

### zkUtilsTest.sh
zkUtils的测试

### consumerTest.sh
该程序用来测试consumer中的rebalance以及自动管理offset功能是否正常。

因为涉及到测试消费涉及到创建topic以及写入messages，所以需要先生成一个partition数小于等于13的topic，名为php-test，并写入消息。

