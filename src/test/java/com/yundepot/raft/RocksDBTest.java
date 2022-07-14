package com.yundepot.raft;

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

    static {
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) throws Exception {
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        RocksDB rocksDB = RocksDB.openReadOnly(options, dataDir, descriptorList, handleList);
        assert (handleList.size() == 1);
        ColumnFamilyHandle defaultHandle = handleList.get(0);

        RocksIterator it = rocksDB.newIterator(defaultHandle);
        for (it.seekToFirst(); it.isValid(); it.next()) {
            byte[] key = it.key();
            byte[] value = it.value();
            System.out.println();
        }
    }
}
