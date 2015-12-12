# RDS_DTS
阿里云数据库RDS之DTS订阅数据消费示例
## 简介
RDS_DTS程序是通过JAVA语言编写的DTS订阅数据消费程序,实现了RDS云数据库到本地数据库的实时同步。 主要包括新增、修改、删除数据，以及DDL语句的实时同步。
DTS客户端SDK文档地址： http://help.aliyun.com/document_detail/dts/User-Document/Data-Subscription/SDK-Introduction.html
## 代码说明
* 1、在运行本程序之前，需要首先进行一次数据库的完全同步，可以借助MySQL图形工具Navicat实现。
* 2、修改db.properties文件中关于云数据库和本地数据库的配置信息。
* 3、运行MainClass.java启动此客户端同步消费程序。
