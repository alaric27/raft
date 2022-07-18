package com.yundepot.raft.statemachine;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;

/**
 * 自定义压缩过滤工厂，在压缩sst文件时，过滤掉已逾期的key
 */
public class ExpireCompactionFilterFactory extends AbstractCompactionFilterFactory {
    @Override
    public AbstractCompactionFilter<?> createCompactionFilter(AbstractCompactionFilter.Context context) {
        // todo  rockdsdb目前只支持通过java包装c++类的方式自定义CompactionFilter。 该功能尚未实现
        return null;
    }

    @Override
    public String name() {
        return "expire";
    }
}
