# raft implement

## raft init
raft创世纪, 需要指定3节点的初始配置，这里是手动传入配置，比如node1=192.168.0.1, node2=192.168.0.2
node3=192.168.0.3, 创建完成之后就可以进入选举阶段

## raft 选举
`candidate timer`的时间按照raft论文里的150~300ms实现，初始的时候各节点的的`term=0`，`timeout`之后将当前
`term`+1，根据初始的成员列表向其他成员发起选举请求。
节点收到选举请求后，根据`term`判断是否投票，term > currentTerm则执行该请求进行投票，每个节点在一轮选举中只会投一次票
投过票的节点不再投票给其他节点，这一轮中也不再进入`candidate state`，如果这一轮没有选出leader，那么进入下一轮选举

## raft log
`raft`定义了一个`SM`，这个`SM`可以运行用户逻辑，用户逻辑可以任何满足SM接口的实现，比如可以包装的lsm/btree甚至简单的
计数器都可以，`SM`通常只需要apply command/recall command

### log design
raft log设计为一个文件组, 有`idx`&`data`文件, `idx`仅存储log元数据, 不存储实际的数据, data实际上是heap, 存储实际数据.
一般检测到data文件大于1GB的时候就可以开始归档了, 会分裂成多个seg, 后台合并进程会检测commitIndex >= seg的文件.
状态机需要可以应用raft发来的log command, 必要的时候也可以recall这些command, 比如`follower`有与当前`leader`不一致的日志, 那么则
需要丢弃这些内容.
状态机还要可以允许生成自己内部的快照, 以供`leader`快速将自己的数据同步给`follower`

### 同步与复制
`raft`协议中规定日志需要有三种`cursor`
- `log_index`位置必须持久化, 记录了当前leader所有的写入记录
- `commit_index`位置可以不持久化, 每次重启时从0开始, 等待leader当选并写入no-op日志, 将新的commitIndex通过appendEntries同步到follower中
- `last_applied`持久化UserSm必须持久化, 通过计算commitIndex - last_applied得出要提交的日志项
- 

## raft client

## 参考资料
- In Search of an Understandable Consensus Algorithm (Extended Version) — Diego Ongaro, John Ousterhout, USENIX ATC 2014
- Consensus: Bridging Theory and Practice — Diego Ongaro, PhD Dissertation, 2014
- Raft Refloated: Do We Have Consensus? — Heidi Howard et al., SIGOPS OSR 2015
- ARC: Analysis of Raft Consensus — Heidi Howard, Cambridge Technical Report 857, 2014
- Verdi: A Framework for Implementing and Verifying Distributed Systems — Wilcox et al., PLDI 2015
- Planning for Change in a Formal Verification of the Raft Consensus Protocol — Woos et al., CPP 2016
- Protocol-Aware Recovery for Consensus-Based Storage — FAST 2018 Best Paper
- Scaling Replicated State Machines with Compartmentalization — PVLDB 2021