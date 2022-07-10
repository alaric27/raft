package com.yundepot.raft.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.RandomAccessFile;

/**
 * @author zhaiyanan
 * @date 2019/7/3 14:06
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotDataFile {
    private String fileName;
    private RandomAccessFile randomAccessFile;
}
