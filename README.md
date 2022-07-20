# raft
raft的简单实现，仅为了学习验证raft算法，没有进行充分测试，非生产可用
论文地址: https://raft.github.io/raft.pdf

# 优化改动点
## 快速定位缺失的日志
在AppendLog的 响应中添加lastLogIndex字段，follower返回最后一条日志的索引，用于缩短向前追溯的过程，加快日志同步。

## 生成快照时保留部分日志
在生成快照成功后，并不是删除所有已应用的日志，而是保留部分已快照的日志，防止清空日志后，落后不多的节点频繁触发快照发送

## no-op补丁
由于新leader不提交非当前任期的日志，当新leader长时间没有新日志时，会导致之前任期的日志长时间得不到提交。
解决方案是选举成功后,新leader产生一条空日志，具体流程是在 Leader 刚选举成功的时候，立即追加一条 no-op 日志，并立即复制到其它节点，no-op 日志一经提交，Leader 前面那些未提交的日志全部间接提交，问题就解决了

## 集群成员变更
采用老节点集且在老节点集中多数达成Committed后再修改内存Configuration的方案。

## 快速生成加载快照
rocksdb 使用checkpoint生成快照时，如果快照和数据目录在同一个磁盘，是走的硬链接的方式, 可以利用这一点快速生成快照
快照加载的时候，需要把快照目录的文件复制到数据目录，由于sst文件是只读的，同样也可以使用硬链接的方式完成文件拷贝，从而快速完成快照加载

## 线性一致性
采用ReadIndex方案，具体流程如下:
1. Leader 在收到客户端读请求时，记录下当前的 commit index，称之为 read index。
2. Leader 向 followers 发起一次心跳包，这一步是为了确保领导权，避免网络分区时少数派 leader 仍处理请求。
3. 等待状态机至少应用到 read index（即 apply index 大于等于 read index）。
4. 执行读请求，将状态机中的结果返回给客户端。

## 过期时间支持
在存储时把有效期字段存储到value的后面。有效期为时间戳格式。
目前实现为 惰性删除 在过了有效期后不会主动删除，而是在查询的时候判断是否超过有效期，如果预期则删除
另外可以rocksdb压缩sst的时候 自定义AbstractCompactionFilterFactory，过滤掉已过期的数据，但是目前rocksdb只支持通过jndi java代码包装c++代码的方式自定义，所以尚未实现该功能

## multi-raft
为了解决单集群容量限制和单集群leader节点的读写压力，所以需要引入 multi-raft。
其核心思想是数据以某种分片方式路由到对应的集群, 每个集群负责一定的分片, 从而可以通过新增集群的方式降低leader的读写压力，增加存储容量的上限。
todo 目前multi-raft 还属于调研学习阶段，尚未实现




