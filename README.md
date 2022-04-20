# Confact
Distributed database based on Raft algorithm


# WHAT TO DO
分布式事务 - Percolator

共识算法 - multi-raft

存储引擎  LevelDB

网络层    net/http（看情况换成fastHTTP或者Gin）

RPC层    GRPC


怎么启动：

multi-raft.json 是multi-raft配置文件。如图所示，【nodes】为节点（机器）个数，【replicate】是副本个数，【nodes_http】是节点对外提供的http端口，【nodes_info】是节点详细信息，
【nodes_rpc】是节点GRPC端口，默认从50010开始递增。

![图片](https://user-images.githubusercontent.com/48211921/163682923-8d3e554e-4ea1-4554-8533-6f3e46d8aff1.png)




该json文件由配置中心统一生成。peers.toml是该机器的配置，也就是配置机器id。
