package com.yundepot.raft;

import org.junit.Before;
import org.junit.Test;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2022/6/30  10:45
 */
public class RocksDBTest {

    static {
        RocksDB.loadLibrary();
    }
    private RocksDB rocksDB;
    private ColumnFamilyHandle defaultHandle;

    @Before
    public void before() throws Exception {
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

        String dataDir = "/Users/zyn/Desktop/candelete/node1/data/";
        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        rocksDB = RocksDB.openReadOnly(options, dataDir, descriptorList, handleList);
        assert (handleList.size() == 1);
        defaultHandle = handleList.get(0);
    }

    @Test
    public void scan() {
        RocksIterator it = rocksDB.newIterator(defaultHandle);
        for (it.seekToFirst(); it.isValid(); it.next()) {
            byte[] key = it.key();
            byte[] value = it.value();
            System.out.println();
        }
    }

    @Test
    public void put() throws Exception {
        for (long i = 1; i < 1000; i++) {
            byte[] key = ("aaa" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = RaftClientTest.getRandomString(10).getBytes(StandardCharsets.UTF_8);
            rocksDB.put(defaultHandle, key, value);
        }
    }
}
