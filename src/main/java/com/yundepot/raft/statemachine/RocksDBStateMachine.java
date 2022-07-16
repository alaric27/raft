package com.yundepot.raft.statemachine;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yundepot.raft.bean.InstallSnapshotRequest;
import com.yundepot.raft.bean.SnapshotDataFile;
import com.yundepot.raft.bean.SnapshotMetadata;
import com.yundepot.raft.common.Constant;
import com.yundepot.raft.config.RaftConfig;
import com.yundepot.raft.exception.RaftException;
import com.yundepot.raft.util.RaftFileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhaiyanan
 * @date 2019/6/15 15:21
 */
@Slf4j
public class RocksDBStateMachine implements StateMachine {
    /**
     * rocksdb data 存储目录
     */
    private final String dataDir;

    /**
     * 快照目录
     */
    private final String snapshotDir;

    /**
     * 快照临时目录
     */
    private final String snapshotTmpDir;

    private final String metadataFile;

    /**
     * 快照元数据
     */
    private SnapshotMetadata metadata;

    /**
     * 是否正在安装快照, leader向follower安装
     */
    private AtomicBoolean installingSnapshot = new AtomicBoolean(false);

    /**
     * 快照打开次数
     */
    private AtomicInteger openCount = new AtomicInteger();
    /**
     * 是否正在生成快照
     */
    private AtomicBoolean takingSnapshot = new AtomicBoolean(false);
    private RocksDB rocksDB;
    private WriteOptions writeOptions = new WriteOptions();
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle configHandle;
    private RaftConfig raftConfig;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBStateMachine(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
        this.dataDir = raftConfig.getRootDir() + "data" + File.separator;
        this.snapshotDir = raftConfig.getRootDir() + "snapshot" + File.separator;
        this.metadataFile = snapshotDir + "metadata";
        this.snapshotTmpDir = raftConfig.getRootDir() + "snapshot.tmp" + File.separator;
        // 关闭rocksdb wal功能，因为raft log可以代替该功能
        writeOptions.setDisableWAL(true);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        try {
            rocksDB.put(defaultHandle, writeOptions, key, value);
        } catch (Exception e) {
            log.error("write data error", e);
            throw new RaftException("write data error", e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return rocksDB.get(defaultHandle, key);
        } catch (Exception e) {
            log.error("get data error", e);
            throw new RaftException("get data error", e);
        }
    }

    @Override
    public void delete(byte[] key) {
        try {
            rocksDB.delete(defaultHandle, key);
        } catch (RocksDBException e) {
            throw new RaftException("delete key error", e);
        }
    }

    @Override
    public void setConfig(byte[] value) {
        try {
            rocksDB.put(configHandle, writeOptions, Constant.CONFIG, value);
        } catch (Exception e) {
            throw new RaftException("write config error", e);
        }
    }

    @Override
    public byte[] getConfig() {
        try {
           return rocksDB.get(configHandle, Constant.CONFIG);
        } catch (Exception e) {
            throw new RaftException("read config error", e);
        }
    }

    @Override
    public void loadSnapshot() {
        try {
            long start = System.currentTimeMillis();
            // 加载快照期间不能写入
            if (rocksDB != null) {
                rocksDB.close();
            }

            // 数据目录如果存在，可直接删除
            File dataFile = new File(this.dataDir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }

            File tmpDir = new File(snapshotTmpDir);
            File ssDir = new File(snapshotDir);
            // 处理临时目录存在，快照目录被删除的情况
            if (tmpDir.exists() && !ssDir.exists()) {
                FileUtils.moveDirectory(tmpDir, ssDir);
            }

            // 将快照目录复制到到数据目录
            if (ssDir.exists()) {
                RaftFileUtils.copySnapshot(ssDir, dataFile);
            }

            DBOptions options = new DBOptions();
            options.setCreateIfMissing(true);
            options.setCreateMissingColumnFamilies(true);

            ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
            familyOptions.setPeriodicCompactionSeconds(raftConfig.getPeriodicCompactionSeconds());
            familyOptions.setLevelCompactionDynamicLevelBytes(true);

            List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
            descriptorList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions));
            descriptorList.add(new ColumnFamilyDescriptor(Constant.CONFIG));

            List<ColumnFamilyHandle> handleList = new ArrayList<>();
            rocksDB = RocksDB.open(options, dataDir, descriptorList, handleList);
            assert (handleList.size() == 2);
            defaultHandle = handleList.get(0);
            configHandle = handleList.get(1);

            reloadMetadata();
            log.info("loadSnapshot success cost: {}", System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("load snapshot error", e);
            throw new RaftException("load snapshot error", e);
        }
    }

    @Override
    public void takeSnapshot(SnapshotMetadata metadata) {
        try {
            // 如果正在安装快照，则忽略本次快照的生成
            if (installingSnapshot.get()) {
                log.info("snapshot is installing, ignore take snapshot");
                return;
            }

            // 有已打开的快照文件，忽略本次快照的生成
            if (openCount.get() > 0) {
                return;
            }

            if (!takingSnapshot.compareAndSet(false, true)) {
                log.info("snapshot is taking, ignore take snapshot");
                return;
            }

            long start = System.currentTimeMillis();
            log.info("begin take snapshot");

            // 清理之前的临时快照文件
            File tmpDir = new File(snapshotTmpDir);
            if (tmpDir.exists()) {
                FileUtils.deleteDirectory(tmpDir);
            }

            Checkpoint checkpoint = Checkpoint.create(rocksDB);
            checkpoint.createCheckpoint(snapshotTmpDir);
            // 写入元数据
            RaftFileUtils.updateFile(snapshotTmpDir + "metadata", JSON.toJSONString(metadata));

            File ssDir = new File(snapshotDir);
            if (ssDir.exists()) {
                FileUtils.deleteDirectory(ssDir);
            }
            FileUtils.moveDirectory(tmpDir, ssDir);
            this.metadata = metadata;
            log.info("take snapshot successful, cost: {}", System.currentTimeMillis() - start);
        } catch (Exception e) {
            throw new RaftException("take snapshot error", e);
        } finally {
            takingSnapshot.set(false);
        }
    }

    @Override
    public boolean installSnapshot(InstallSnapshotRequest request) {
        if (takingSnapshot.get()) {
            return false;
        }

        if (!installingSnapshot.compareAndSet(false, true)) {
            return false;
        }

        RandomAccessFile accessFile = null;
        try {
            File tmpDir = new File(snapshotTmpDir);
            if (request.isFirst() && tmpDir.exists()) {
                FileUtils.deleteDirectory(tmpDir);
            }

            RaftFileUtils.createDir(snapshotTmpDir);
            File tmpDataFile = new File(snapshotTmpDir + request.getFileName());

            accessFile = FileUtil.createRandomAccessFile(tmpDataFile, FileMode.rw);
            accessFile.seek(request.getOffset());
            accessFile.write(request.getData());

            if (request.isDone()) {
                // 删除原快照文件
                File snapshotDirFile = new File(snapshotDir);
                if (snapshotDirFile.exists()) {
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(tmpDir, snapshotDirFile);
                loadSnapshot();
            }
        } catch (Exception e) {
            log.warn("install snapshot error", e);
            return false;
        } finally {
            RaftFileUtils.closeFile(accessFile);
            installingSnapshot.set(false);
        }
        return true;
    }

    @Override
    public SnapshotMetadata getMetadata() {
        return metadata;
    }

    @Override
    public List<SnapshotDataFile> openSnapshotDataFile() {
        openCount.incrementAndGet();
        if (takingSnapshot.get()) {
            log.debug("leader is take snapshot, please send install snapshot request later");
            return null;
        }
        List<SnapshotDataFile> list = new ArrayList<>();
        List<String> fileList = RaftFileUtils.getSortedFilesInDirectory(snapshotDir);
        for (String filePath : fileList) {
            RandomAccessFile randomAccessFile = RaftFileUtils.openFile(filePath, "r");
            list.add(new SnapshotDataFile(FileUtil.getName(filePath), randomAccessFile));
        }
        return list;
    }

    @Override
    public void closeSnapshotDataFile(List<SnapshotDataFile> list) {
        try {
            if (CollectionUtils.isEmpty(list)) {
                return;
            }
            for (SnapshotDataFile dataFile : list) {
                RaftFileUtils.closeFile(dataFile.getRandomAccessFile());
            }
        } finally {
            openCount.incrementAndGet();
        }
    }

    @Override
    public void shutdown() {
        defaultHandle.close();
        rocksDB.close();
    }

    private void reloadMetadata() throws IOException {
        File file = new File(metadataFile);
        if (file.exists()) {
            String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            metadata = JSON.parseObject(content, new TypeReference<SnapshotMetadata>(){}.getType());
        }

        if (metadata == null) {
            metadata = new SnapshotMetadata();
        }
    }
}
