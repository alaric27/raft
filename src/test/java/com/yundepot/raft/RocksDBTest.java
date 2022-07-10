package com.yundepot.raft;

import com.alibaba.fastjson.JSON;
import com.yundepot.raft.bean.SnapshotMetadata;
import com.yundepot.raft.common.Constant;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2022/6/30  10:45
 */
public class RocksDBTest {
    private static String dataDir = "/Users/zyn/Desktop/candelete/node1/data/";

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();

        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);

        List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        descriptorList.add(new ColumnFamilyDescriptor("config".getBytes(StandardCharsets.UTF_8)));

        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        RocksDB rocksDB = RocksDB.openReadOnly(options, dataDir, descriptorList, handleList);
        assert (handleList.size() == 2);
        ColumnFamilyHandle defaultHandle = handleList.get(0);
        ColumnFamilyHandle configHandle = handleList.get(1);
        System.out.println(rocksDB.getColumnFamilyMetaData(defaultHandle).size() / 1024 /1024);

        byte[] bytes = rocksDB.get(configHandle, Constant.METADATA);
        if (bytes != null) {
            SnapshotMetadata metadata = JSON.parseObject(bytes, SnapshotMetadata.class);
            System.out.println(JSON.toJSONString(metadata));
        }
    }
}
