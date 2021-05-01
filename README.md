# Kafka Tools


可以通过tab来自动补全；通过help来查看帮助；使用quit退出程序

## 查找 offset

查找offset时需要指定时间。时间的格式可以为
* N小时之前，如2h-ago
* N分钟之前，如2h-ago
* N秒之前,如2s-ago
* 指定时间戳，如 1493373225535

查找offset的对象可以是topic，也可以是分区。是分区的话，需要以 `partitionId@topic`的形式指定。例如：`1@apple`


 例如：
 * `select offset from my_topic where time = 1h-ago"`
 * `select offset from 1@my_topic where time = 0s-ago` 可以用来查找一个topic在当前时刻最大offset的下一个offset

## 查找消息

支持通过指定offset来获取消息

### 通过offset获取消息

`select message from 1@my_topic where offset = 19000`

### 获取一个topic最近一条消息

`select latest message from my_topic`


## 查看一个topic的情况

`describe topic_name`

包括：
* 分区个数，每个分区的副本位置，当前leader
* 每个分区最小的offset
* 每个分区最大的offset


## 列出所有topic

`list topics`

## 查看consumer group的情况

### 查看一个consumer group当前的情况

`describe group consumer_group_id`

### 监控一个consumer group的消费情况

`watch consumer_group_name`

这个命令会把这个consumer group加入监听列表， 只要它有committed offset的更新，就把它打印出来。可以用ctrl + D 来退出监听；或者使用`stop watching consumer_group_name`来停止上一次加入的
监听。

## 列出可用命令

`help`
