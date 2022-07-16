package com.yundepot.raft;

import cn.hutool.core.lang.mutable.MutableInt;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yundepot.oaa.common.AbstractLifeCycle;
import com.yundepot.oaa.common.NamedThreadFactory;
import com.yundepot.oaa.config.GenericOption;
import com.yundepot.raft.bean.*;
import com.yundepot.raft.common.*;
import com.yundepot.raft.config.RaftConfig;
import com.yundepot.raft.service.*;
import com.yundepot.raft.statemachine.RocksDBStateMachine;
import com.yundepot.raft.statemachine.StateMachine;
import com.yundepot.raft.store.ClusterConfigStore;
import com.yundepot.raft.store.LogStore;
import com.yundepot.raft.store.NodeStateStore;
import com.yundepot.raft.store.RocksDBLogStore;
import com.yundepot.raft.util.ByteUtil;
import com.yundepot.raft.util.ClusterUtil;
import com.yundepot.raft.util.LockUtil;
import com.yundepot.raft.util.RandomUtil;
import com.yundepot.rpc.RpcServer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
@Slf4j
@Data
public class RaftNode extends AbstractLifeCycle {
    /**
     * 配置信息
     */
    private final RaftConfig raftConfig;

    /**
     * 集群配置
     */
    private ClusterConfig clusterConfig;

    /**
     * 节点, 不包含本节点
     */
    private Map<Integer, Peer> peerMap = new ConcurrentHashMap<>();

    /**
     * 当前服务
     */
    private Server localServer;

    /**
     * 状态机
     */
    private StateMachine stateMachine;

    /**
     * 日志文件
     */
    private LogStore logStore;

    /**
     * 节点状态存储
     */
    private NodeStateStore nodeStageStore;

    /**
     * 节点状态
     */
    private RaftRole state = RaftRole.FOLLOWER;

    /**
     * 当前任期
     */
    private volatile long currentTerm;

    /**
     * 当前任期投票投给了谁
     */
    private int votedFor;

    /**
     * 主节点id
     */
    private volatile int leaderId;

    /**
     * 被提交的最大日志条目的索引值
     */
    private volatile long commitIndex;

    /**
     * 被状态机执行的最大日志条目的索引值
     */
    private volatile long lastAppliedIndex;

    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = lock.newCondition();
    private Condition catchUpCondition = lock.newCondition();
    private Condition appendCondition = lock.newCondition();

    /**
     * 追加日志的线程池
     */
    private ExecutorService executorService;

    /**
     * 快照选举及心跳的线程池
     */
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * 选举的ScheduledTimer
     */
    private ScheduledTimer electionTimer;
    private ScheduledTimer heartbeatTimer;
    private RpcServer rpcServer;
    private ClusterConfigStore clusterConfigStore;

    public RaftNode(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
        this.clusterConfig = ClusterUtil.parserCluster(raftConfig.getCluster());
        this.localServer = ClusterUtil.getServer(clusterConfig, raftConfig.getServer());
        this.stateMachine = new RocksDBStateMachine(raftConfig.getRootDir());
        this.nodeStageStore = new NodeStateStore(raftConfig.getRootDir());
        this.logStore = new RocksDBLogStore(raftConfig);
        this.clusterConfigStore = new ClusterConfigStore(raftConfig.getRootDir());

        // 初始化rpc服务
        initRpc();

        // 加载日志文件
        load();

        // 初始化集群节点
        clusterConfig.getServerList().forEach(server -> {
            if (!peerMap.containsKey(server.getServerId()) && server.getServerId() != localServer.getServerId()) {
                Peer peer = new Peer(server);
                peer.setNextIndex(getLastLogIndex() + 1);
                peerMap.put(server.getServerId(), peer);
            }
        });

        executorService = new ThreadPoolExecutor(raftConfig.getTpMin(), raftConfig.getTpMax(), 60, TimeUnit.SECONDS,
                                                    new LinkedBlockingQueue<>(), new NamedThreadFactory("raft", true));
        scheduledExecutorService = Executors.newScheduledThreadPool(2, new NamedThreadFactory("raftScheduled", true));
        electionTimer = new ScheduledTimer("election", raftConfig.getElectionTimeout(), () -> startPreVote(), RandomUtil::getRangeLong);
        heartbeatTimer = new ScheduledTimer("heartbeat", raftConfig.getHeartbeatPeriod(), () -> sendHeartbeat());
    }

    /**
     * 加载日志文件及快照文件
     * 先加载快照文件，然后追加剩余的日志
     */
    private void load() {
        logStore.loadLog();
        nodeStageStore.load();
        stateMachine.loadSnapshot();
        clusterConfigStore.load();

        this.currentTerm = nodeStageStore.get().getCurrentTerm();
        this.votedFor = nodeStageStore.get().getVotedFor();
        this.commitIndex = stateMachine.getMetadata().getLastIncludedIndex();
        this.lastAppliedIndex = commitIndex;

        // 如果快照中的集群配置不为空，则使用快照中的
        byte[] bytes = stateMachine.getConfig();
        if (bytes != null) {
            this.clusterConfig = JSON.parseObject(bytes, ClusterConfig.class);
        }

        // 如果持久化的集群配置不为空，则以其为准
        if (clusterConfigStore.get() != null) {
            this.clusterConfig = clusterConfigStore.get();
        }
    }

    private void initRpc() {
        this.rpcServer = new RpcServer(localServer.getPort());
        this.rpcServer.option(GenericOption.TCP_HEARTBEAT_SWITCH, false);
        // 注册Raft节点之间相互调用的服务
        RaftService raftService = new RaftServiceImpl(this);
        rpcServer.addService(RaftService.class.getName(), raftService);

        // 注册集群管理服务
        RaftAdminService raftAdminService = new RaftAdminServiceImpl(this);
        rpcServer.addService(RaftAdminService.class.getName(), raftAdminService);

        // 注册应用自己提供的服务
        PairService pairService = new PairServiceImpl(this, stateMachine);
        rpcServer.addService(PairService.class.getName(), pairService);
    }

    @Override
    public void start() {
        super.start();
        this.rpcServer.start();
        log.info("raft start with {}:{}", localServer.getHost(), localServer.getPort());
        // 建立快照
        scheduledExecutorService.scheduleWithFixedDelay(() -> takeSnapshot(),raftConfig.getSnapshotPeriod(),
                raftConfig.getSnapshotPeriod(), TimeUnit.SECONDS);
        electionTimer.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.rpcServer.shutdown();
        executorService.shutdown();
        scheduledExecutorService.shutdown();
        logStore.shutdown();
        stateMachine.shutdown();
    }

    private void startPreVote() {
        LockUtil.runWithLock(lock, () -> {
            // 如果只有一个节点, 直接成为领导者
            if (clusterConfig.getServerList().size() == 1) {
                becomeLeader();
                return;
            }
            leaderId = 0;
            state = RaftRole.PRE_CANDIDATE;

            List<Peer> peerList = peerMap.values().stream().filter(peer -> ClusterUtil.containsServer(clusterConfig, peer.getServer().getServerId())).collect(Collectors.toList());
            peerList.forEach(peer -> peer.setVoteGranted(false));
            peerList.forEach(peer -> scheduledExecutorService.execute(() -> preVote(peer)));
        });
    }

    private void preVote(Peer peer) {
        VoteRequest request = new VoteRequest();
        LockUtil.runWithLock(lock, () -> {
            peer.setVoteGranted(false);
            request.setCandidateId(localServer.getServerId());
            request.setTerm(currentTerm);
            request.setLastLogTerm(getLastLogTerm());
            request.setLastLogIndex(logStore.getLastLogIndex());
        });

        log.info("preVote request {}, to peer {}", request, peer.getServer().getServerId());
        VoteResponse response = peer.getPeerClient().preVote(request);
        log.info("preVote response {}, from peer {}", response, peer.getServer().getServerId());

        LockUtil.runWithLock(lock, () -> {
            if (currentTerm != request.getTerm() || state != RaftRole.PRE_CANDIDATE) {
                log.info("ignore preVote response, request: {}, response: {}", request, response);
                return;
            }
            peer.setVoteGranted(response.isVoteGranted());
            if (response.getTerm() > currentTerm) {
                stepDown(response.getTerm());
            } else if (response.isVoteGranted()) {
                // 统计票数
                int voteGrantedNum = (int) peerMap.values().stream().filter(p -> p.isVoteGranted()).count() + 1;
                log.info("pre vote num ={}", voteGrantedNum);
                if (voteGrantedNum > clusterConfig.getServerList().size() / 2) {
                    startVote();
                }
            }
        });
    }

    /**
     * 开始投票
     */
    private void startVote() {
        LockUtil.runWithLock(lock, () -> {
            currentTerm++;
            log.info("Running for election in term {}", currentTerm);
            state = RaftRole.CANDIDATE;
            votedFor = localServer.getServerId();
            nodeStageStore.update(currentTerm, votedFor);

            // 过滤掉不在集群内的节点
            List<Peer> peerList = peerMap.values().stream().filter(peer -> ClusterUtil.containsServer(clusterConfig, peer.getServer().getServerId())).collect(Collectors.toList());
            // 开始一轮投票前，先清除投票数
            peerList.forEach(peer -> peer.setVoteGranted(false));
            peerList.forEach(peer -> scheduledExecutorService.execute(() -> requestVote(peer)));
        });
    }

    /**
     * 发起投票
     * @param peer
     */
    private void requestVote(Peer peer) {
        VoteRequest request = new VoteRequest();
        LockUtil.runWithLock(lock, () -> {
            peer.setVoteGranted(false);
            request.setCandidateId(localServer.getServerId());
            request.setTerm(currentTerm);
            request.setLastLogTerm(getLastLogTerm());
            request.setLastLogIndex(logStore.getLastLogIndex());
        });

        log.info("vote request {}, to peer {}", request, peer.getServer().getServerId());
        VoteResponse response = peer.getPeerClient().vote(request);
        log.info("vote response {}, from peer {}", response, peer.getServer().getServerId());

        // 投票响应
        LockUtil.runWithLock(lock, () -> {
            if (currentTerm != request.getTerm() || state != RaftRole.CANDIDATE) {
                log.info("ignore vote response, request: {}, response: {}", request, response);
                return;
            }
            peer.setVoteGranted(response.isVoteGranted());
            if (response.getTerm() > currentTerm) {
                stepDown(response.getTerm());
            } else if (response.isVoteGranted()){
                // 统计票数
                int voteGrantedNum = (int) peerMap.values().stream().filter(p -> p.isVoteGranted()).count() + 1;
                log.info("getVoteGrantedNum={}", voteGrantedNum);
                if (voteGrantedNum > clusterConfig.getServerList().size() / 2) {
                    becomeLeader();
                }
            }
        });
    }

    /**
     * 成为领导者
     */
    private void becomeLeader() {
        state = RaftRole.LEADER;
        leaderId = localServer.getServerId();

        // 成为leader 后初始化 nextIndex和 matchIndex
        peerMap.values().forEach(peer -> {
            peer.setNextIndex(getLastLogIndex() + 1);
            peer.setMatchIndex(0);
        });

        electionTimer.stop();
        heartbeatTimer.start();
        // no-op，成为leader后进行一次空日志提交，从而隐式地提交之前任期的日志
        replicate(null, LogType.DATA);
        log.info("server:{} become leader, currentTerm: {}", leaderId, currentTerm);
    }

    /**
     * 下台
     * 需要在锁中
     * @param newTerm
     */
    public void stepDown(long newTerm) {
        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            votedFor = Constant.ZERO;
            leaderId = Constant.ZERO;
            nodeStageStore.update(currentTerm, votedFor);
        }
        state = RaftRole.FOLLOWER;
        heartbeatTimer.stop();
        electionTimer.reset();
    }

    /**
     * 发送心跳
     */
    public void sendHeartbeat() {
        peerMap.values().forEach(peer -> executorService.submit(() -> appendLog(peer)));
    }

    /**
     * leader 向其他节点发送心跳或追加日志
     * @param peer
     */
    public void appendLog(Peer peer) {
        // 如果正在向当前peer安装快照，则不向其发送数据
        if (peer.isInstallingSnapshot()) {
            return;
        }

        AppendLogRequest request = new AppendLogRequest();
        long packLastIndex;
        // 检查是否需要向peer发送快照
        if (checkInstallSnapshot(peer)) {
            installSnapshot(peer);
        }

        lock.lock();
        try {
            long prevLogIndex = peer.getNextIndex() - 1;
            long prevLogTerm = getEntryTerm(prevLogIndex);;
            request.setLeaderId(localServer.getServerId());
            request.setTerm(currentTerm);
            request.setPrevLogTerm(prevLogTerm);
            request.setPrevLogIndex(prevLogIndex);
            packLastIndex = packLog(peer.getNextIndex(), request);
            request.setLeaderCommit(commitIndex);
        } finally {
            lock.unlock();
        }

        long start = System.currentTimeMillis();
        log.debug("appendLog request {} to {}", request, peer.getServer().getServerId());
        AppendLogResponse response = peer.getPeerClient().appendLog(request);
        log.debug("appendLog response {} from {}, cost: {}", response, peer.getServer(), System.currentTimeMillis() - start);
        appendLogResponse(response, peer, packLastIndex);
    }

    /**
     * 添加日志的响应
     * @param response
     */
    private void appendLogResponse(AppendLogResponse response, Peer peer, long packLastIndex) {
        lock.lock();
        try {
            if (response == null) {
                log.warn("appendLog to peer {}:{} failed", peer.getServer().getHost(), peer.getServer().getPort());
                // 如果不在集群中，说明添加peer失败了，直接删除即可
                if (!ClusterUtil.containsServer(clusterConfig, peer.getServer().getServerId())) {
                    peerMap.remove(peer.getServer().getServerId());
                    peer.getPeerClient().shutdown();
                }
                return;
            }

            if (response.getTerm() > currentTerm) {
                stepDown(response.getTerm());
                return;
            }

            if (response.isSuccess()) {
                peer.setMatchIndex(packLastIndex);
                peer.setNextIndex(peer.getMatchIndex() + 1);
                peer.setLastResponseStatus(true);
                peer.setLastResponseTime(System.currentTimeMillis());
                if (ClusterUtil.containsServer(clusterConfig, peer.getServer().getServerId())) {
                    advanceCommitIndex();
                } else {
                    // 处理集群新增节点的情况
                    if (logStore.getLastLogIndex() - peer.getMatchIndex() <= raftConfig.getCatchupGap()) {
                        log.debug("peer catch up the leader");
                        peer.setCatchUp(true);
                        catchUpCondition.signalAll();
                    }
                }
                appendCondition.signalAll();
            } else {
                // 如果失败了, 往前追溯日志
                long nextIndex = Math.min(response.getLastLogIndex() + 1, peer.getNextIndex() - 1);
                peer.setNextIndex(Math.max(1, nextIndex));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 周期性生成快照
     */
    private void takeSnapshot() {
        SnapshotMetadata metadata = new SnapshotMetadata();
        lock.lock();
        try {
            // 日志太小不处理
            if (logStore.getFileSize() < raftConfig.getSnapshotMinLogSize()) {
                return;
            }
            LogEntry entry = logStore.getEntry(lastAppliedIndex);
            // 如果 entry 为空表明lastAppliedIndex之前的日志已经生成过快照了
            if (entry == null) {
                return;
            }

            metadata.setLastIncludedIndex(lastAppliedIndex);
            metadata.setLastIncludedTerm(entry.getTerm());
        } finally {
            lock.unlock();
        }

        // 不需加锁, 因为有原子状态takingSnapshot保证不能并发生成快照
        stateMachine.takeSnapshot(metadata);

        long lastIncludedIndex = LockUtil.getWithLock(lock, () -> stateMachine.getMetadata().getLastIncludedIndex());
        // 删除已快照的日志
        if (lastIncludedIndex > 0 && logStore.getFirstLogIndex() <= lastIncludedIndex) {
            logStore.deletePrefix(lastIncludedIndex + 1 - raftConfig.getKeepLogNum());
        }
    }

    public long getLastLogTerm() {
        long lastLogIndex = logStore.getLastLogIndex();
        // log为空
        if (lastLogIndex == 0) {
            return stateMachine.getMetadata().getLastIncludedTerm();
        }
        return getEntryTerm(lastLogIndex);
    }

    public long getLastLogIndex() {
        long lastLogIndex = logStore.getLastLogIndex();
        // log为空
        if (lastLogIndex == 0) {
            return stateMachine.getMetadata().getLastIncludedIndex();
        }
        return lastLogIndex;
    }

    /**
     * leader更新提交数据, 在锁中
     */
    private void advanceCommitIndex() {
        int peerNum = clusterConfig.getServerList().size();
        List<Long> list = new ArrayList<>();
        peerMap.values().forEach(peer -> list.add(peer.getMatchIndex()));
        list.add(logStore.getLastLogIndex());
        Collections.sort(list);
        long newCommitIndex = list.get((peerNum - 1) / 2);
        log.debug("newCommitIndex={}, oldCommitIndex={}", newCommitIndex, commitIndex);

        if (commitIndex >= newCommitIndex) {
            return;
        }

        // 如果该索引不在当前leader的任期，则不能提交
        if (getEntryTerm(newCommitIndex) != currentTerm) {
            log.info("newCommitIndex = {}, newCommitIndexTerm is not currentTerm, newCommitIndexTerm={}, currentTerm={}", newCommitIndex, getEntryTerm(newCommitIndex), currentTerm);
            return;
        }

        // 同步到状态机
        for (long index = lastAppliedIndex + 1; index <= newCommitIndex; index++) {
            LogEntry entry = logStore.getEntry(index);
            applyLogEntry(entry);
            lastAppliedIndex = index;
            commitIndex = index;
        }
        log.debug("commitIndex={} lastAppliedIndex={}", commitIndex, lastAppliedIndex);
        commitIndexCondition.signalAll();
    }

    private void applyLogEntry(LogEntry entry) {
        // 处理空日志的场景
        if (entry.getData() == null || entry.getData().length <= 0) {
            return;
        }
        if (entry.getLogType() == LogType.DATA.getValue()) {
            Pair pair = ByteUtil.decode(entry.getData());
            stateMachine.put(pair.getKey(), pair.getValue());
        } else if (entry.getLogType() == LogType.CONFIG.getValue()) {
            // 应用到集群配置
            applyConfig(entry);
        }
    }

    /**
     * 应用日志到集群配置
     * @param entry
     */
    public void applyConfig(LogEntry entry) {
        final List<Server> serverList = JSON.parseObject(entry.getData(), new TypeReference<List<Server>>(){}.getType());
        // 如果新集群已经不包含当前节点
        if (!ClusterUtil.containsServer(serverList, localServer.getServerId())) {
            Set<Server> newServers = new HashSet<>();
            newServers.add(localServer);
            clusterConfig.setServerList(serverList);
            stateMachine.putConfig(JSON.toJSONBytes(clusterConfig));
            clusterConfigStore.update(clusterConfig);
            stepDown(currentTerm);
            peerMap.values().forEach(peer -> peer.getPeerClient().shutdown());
            peerMap.clear();
            return;
        }

        // 添加新节点
        for (Server server : serverList) {
            if (!peerMap.containsKey(server.getServerId()) && server.getServerId() != localServer.getServerId()) {
                Peer peer = new Peer(server);
                peer.setNextIndex(logStore.getLastLogIndex() + 1);
                peerMap.put(server.getServerId(), peer);
            }
        }

        // 删除不在集群内的节点
        Set<Integer> toDelete = peerMap.keySet().stream().filter(serverId -> !ClusterUtil.containsServer(serverList, serverId)).collect(Collectors.toSet());
        toDelete.forEach(serverId -> {
            Peer peer = peerMap.remove(serverId);
            peer.getPeerClient().shutdown();
        });

        clusterConfig.setServerList(serverList);
        stateMachine.putConfig(JSON.toJSONBytes(clusterConfig));
        clusterConfigStore.update(clusterConfig);
    }

    /**
     * 打包日志
     * @param nextIndex
     * @param request
     * @return 包中最后日志索引
     */
    private long packLog(long nextIndex, AppendLogRequest request) {
        long lastIndex = nextIndex - 1;
        if (nextIndex < logStore.getFirstLogIndex()) {
            return lastIndex;
        }
        long lastLogIndex = logStore.getLastLogIndex();
        long size = 0;
        for (long i = nextIndex; i <= lastLogIndex; i++) {
            LogEntry entry = logStore.getEntry(i);
            request.getLogEntryList().add(entry);
            lastIndex = i;
            // 超出每次请求最大限制
            size += entry.getData() == null ? 0 : entry.getData().length;
            if (size >= raftConfig.getMaxSizePerRequest()) {
                break;
            }
        }
        return lastIndex;
    }

    public long getEntryTerm(long index) {
        SnapshotMetadata metadata = stateMachine.getMetadata();
        // 如果前一个日志已经在快照中
        if (metadata.getLastIncludedIndex() == index) {
            return metadata.getLastIncludedTerm();
        } else {
            LogEntry entry = logStore.getEntry(index);
            if (entry == null) {
                return 0;
            }
            return entry.getTerm();
        }
    }

    /**
     * 检查是否需要向peer发送快照
     * @param peer
     */
    private boolean checkInstallSnapshot(Peer peer) {
        lock.lock();
        try {
            // 如果日志已经存在快照中，需要向peer发送快照
            long firstLogIndex = logStore.getFirstLogIndex();
            long nextIndex = peer.getNextIndex();
            if (nextIndex < firstLogIndex || (firstLogIndex == 0 && nextIndex <= stateMachine.getMetadata().getLastIncludedIndex())) {
                log.info("need install snapshot to server = {}, firstLogIndex = {}, nextIndex = {}, lastIncludedIndex = {}",
                        peer.getServer().getServerId(), firstLogIndex, nextIndex, stateMachine.getMetadata().getLastIncludedIndex());
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    private void installSnapshot(Peer peer) {

        log.info("begin send install snapshot request to server={}", peer.getServer().getServerId());
        long lastIncludedIndex = 0;
        List<SnapshotDataFile> list = null;
        try {
            peer.setInstallingSnapshot(true);
            list = stateMachine.openSnapshotDataFile();
            if (CollectionUtils.isEmpty(list)) {
                return;
            }
            long lastOffset = 0;
            long lastLength = 0;
            MutableInt index = new MutableInt(0);
            while (true) {
                InstallSnapshotRequest request = buildInstallSnapshotRequest(list, index, lastOffset, lastLength);
                if (request == null) {
                    log.warn("snapshot request == null");
                    return;
                }

                log.info("install snapshot request: {} to {}", request, peer.getServer().getServerId());
                InstallSnapshotResponse response = peer.getPeerClient().installSnapshot(request);
                log.info("install snapshot response : {} from {}", response, peer.getServer().getServerId());
                if (response == null || response.getResCode() != ResponseCode.SUCCESS.getValue()) {
                    return;
                }

                lastOffset = request.getOffset();
                lastLength = request.getData().length;
                lastIncludedIndex = request.getLastIncludedIndex();
                if (request.isDone()) {
                    break;
                }
            }
        } finally {
            peer.setInstallingSnapshot(false);
            stateMachine.closeSnapshotDataFile(list);
        }

        long nextIndex = lastIncludedIndex + 1;
        LockUtil.runWithLock(lock, () -> peer.setNextIndex(nextIndex));
        log.info("end send install snapshot request to server={}", peer.getServer().getServerId());
    }

    private InstallSnapshotRequest buildInstallSnapshotRequest(List<SnapshotDataFile> list, MutableInt index, long lastOffset, long lastLength) {
        InstallSnapshotRequest request = new InstallSnapshotRequest();
        try {
            SnapshotDataFile snapshotDataFile = list.get(index.get());
            RandomAccessFile dataFile = snapshotDataFile.getRandomAccessFile();
            long fileLength = dataFile.length();
            String fileName = snapshotDataFile.getFileName();

            long currentOffset = lastOffset + lastLength;
            // 本次请求发送的数据大小
            int dataSize = raftConfig.getMaxSizePerRequest();

            // 如果当前文件还没发送完
            if (lastOffset + lastLength < fileLength) {
                // 如果剩余的文件不足 maxSizePerRequest
                if (raftConfig.getMaxSizePerRequest() > fileLength - (lastOffset + lastLength)) {
                    dataSize = (int) (fileLength - (lastOffset + lastLength));
                }
            } else {
                // 发送一个文件
                index.increment();
                dataFile = list.get(index.get()).getRandomAccessFile();
                fileName = list.get(index.get()).getFileName();
                currentOffset = 0;
                dataSize = Math.min(dataSize, (int) dataFile.length());
            }

            byte[] currentData = new byte[dataSize];
            dataFile.seek(currentOffset);
            dataFile.read(currentData);
            request.setData(currentData);
            request.setFileName(fileName);
            request.setOffset(currentOffset);
            // 最后一个文件最后一次发送
            if (index.get() == list.size() - 1 && currentOffset + dataSize >= dataFile.length()) {
                request.setDone(true);
                SnapshotMetadata metadata = stateMachine.getMetadata();
                request.setLastIncludedIndex(metadata.getLastIncludedIndex());
                request.setLastIncludedTerm(metadata.getLastIncludedTerm());
            }

            // 第一次发送
            if (index.get() == 0 && currentOffset == 0) {
                request.setFirst(true);
            }
            request.setTerm(currentTerm);
            request.setLeaderId(localServer.getServerId());
        } catch (Exception ex) {
            log.warn("buildInstallSnapshotRequest exception:", ex);
            return null;
        }
        return request;
    }

    public boolean installSnapshot(InstallSnapshotRequest request) {
        // 由原子状态installingSnapshot保证不会并发安装快照
        if (!stateMachine.installSnapshot(request)) {
            return false;
        }

        if (request.isDone()) {
            // 成功安装快照后清除所有日志
            logStore.deleteSuffix(0);

            // 安装快照后更新集群配置
            lock.lock();
            try {
                byte[] bytes = stateMachine.getConfig();
                if (bytes != null) {
                    this.clusterConfig = JSON.parseObject(bytes, ClusterConfig.class);
                    clusterConfigStore.update(clusterConfig);
                }
            } finally {
                lock.unlock();
            }
        }
        return true;
    }

    /**
     * leader 复制数据到其他节点
     * @param data
     * @param entryType
     * @return
     */
    public boolean replicate(byte[] data, LogType entryType) {
        lock.lock();
        long newLastLogIndex;
        try {
            if (state != RaftRole.LEADER) {
                log.debug("current node is not leader, ignore request");
                return false;
            }
            newLastLogIndex = getLastLogIndex() + 1;
            LogEntry logEntry = new LogEntry();
            logEntry.setTerm(currentTerm);
            logEntry.setIndex(newLastLogIndex);
            logEntry.setLogType(entryType.getValue());
            logEntry.setData(data);

            List<LogEntry> entries = new ArrayList<>();
            entries.add(logEntry);
            logStore.append(entries);

            // 只有一个节点
            if (clusterConfig.getServerList().size() == 1) {
                applyLogEntry(logEntry);
                lastAppliedIndex = logEntry.getIndex();
                commitIndex = lastAppliedIndex;
                return true;
            }
            peerMap.values().forEach(peer -> executorService.submit(() -> appendLog(peer)));
            // 主节点写成功后，就返回。
            if (raftConfig.isAsyncReplicate()) {
                return true;
            }

            // 等待日志同步
            return awaitCommit(newLastLogIndex);
        } catch (Exception ex) {
            log.error("replicate error", ex);
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待日志提交到index索引处
     * @param index
     */
    public boolean awaitCommit(long index) {
        long start = System.currentTimeMillis();
        while (lastAppliedIndex < index) {
            try {
                if (System.currentTimeMillis() - start >= raftConfig.getMaxAwaitTimeout()) {
                    break;
                }
                commitIndexCondition.await(raftConfig.getMaxAwaitTimeout(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("await index {} interrupted", index, e);
            }
        }

        if (lastAppliedIndex < index) {
            return false;
        }
        return true;
    }

    /**
     * 等待集群过半响应
     * @return
     */
    public boolean awaitAppend() {
        int quorum = (clusterConfig.getServerList().size() + 1 ) / 2;
        long start = System.currentTimeMillis();
        while (peerMap.values().stream().filter(p -> p.isLastResponseStatus()).count() + 1 < quorum) {
            try {
                if (System.currentTimeMillis() - start >= raftConfig.getMaxAwaitTimeout()) {
                    break;
                }
                appendCondition.await(raftConfig.getMaxAwaitTimeout(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("await append interrupted", e);
            }
        }

        if (peerMap.values().stream().filter(p -> p.isLastResponseStatus()).count() + 1 < quorum) {
            return false;
        }
        return true;
    }


    /**
     * follower 接收来自leader的日志, 在锁中
     * @param request
     */
    public void appendLogEntry(AppendLogRequest request) {
        List<LogEntry> entries = new ArrayList<>();
        long firstLogIndex = logStore.getFirstLogIndex();
        long lastLogIndex = logStore.getLastLogIndex();

        for (LogEntry entry : request.getLogEntryList()) {
            long index = entry.getIndex();
            if (index < firstLogIndex) {
                continue;
            }
            // 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的日志
            if (lastLogIndex >= index) {
                if (getEntryTerm(index) == entry.getTerm()) {
                    continue;
                }
                logStore.deleteSuffix(index - 1);
            }
            entries.add(entry);
        }

        // 追加日志
        if (!CollectionUtils.isEmpty(entries)) {
            logStore.append(entries);
        }
        // 更新已提交日志到状态机
        advanceCommitIndex(Math.min(request.getLeaderCommit(), getLastLogIndex()));
    }

    /**
     * follower 更新提交数据
     * @param newCommitIndex
     */
    private void advanceCommitIndex(long newCommitIndex) {
        for (long index = lastAppliedIndex + 1; index <= newCommitIndex; index++) {
            LogEntry entry = logStore.getEntry(index);
            applyLogEntry(entry);
            lastAppliedIndex = index;
            commitIndex = index;
        }
    }
}
