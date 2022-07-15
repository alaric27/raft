package com.yundepot.raft.util;

import com.yundepot.raft.exception.RaftException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
@Slf4j
public class RaftFileUtils {

    public static List<String> getSortedFilesInDirectory(String dirName) {
        List<String> fileList = new ArrayList<>();
        File dir = new File(dirName);
        File[] files = dir.listFiles();
        for (File file : files) {
            if (!file.isDirectory()) {
                fileList.add(file.getAbsolutePath());
            }
        }
        Collections.sort(fileList);
        return fileList;
    }

    public static void createDir(String path) {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public static RandomAccessFile openFile(String fileName, String mode) {
        try {
            File file = new File(fileName);
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException ex) {
            log.warn("file not fount, file={}", fileName);
            throw new RuntimeException("file not found, file=" + fileName);
        }
    }

    public static void closeFile(RandomAccessFile randomAccessFile) {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ex) {
            log.warn("close file error, msg={}", ex.getMessage());
        }
    }

    public static void updateFile(String fileName, String content) {
        try {
            String tempFileName =  fileName + ".tmp";
            File tempFile = new File(tempFileName);
            File file = new File(fileName);
            if (tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
            tempFile.createNewFile();
            FileUtils.writeStringToFile(tempFile, content, StandardCharsets.UTF_8);

            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
            FileUtils.moveFile(tempFile, file);
        } catch (Exception e) {
            log.error("update file error", e);
            throw new RaftException("update file error, fileName = " + fileName, e);
        }
    }

    public static void copySnapshot(File srcDir, File destDir) throws IOException {
        if (!srcDir.exists()) {
            throw new FileNotFoundException();
        }

        if (!destDir.exists()) {
            destDir.mkdirs();
        }

        File[] files = srcDir.listFiles();
        for (File file : files) {
            String fileName = file.getName();
            File link = new File(destDir.getAbsolutePath() + File.separator + file.getName());
            if (fileName.endsWith(".sst")) {
                Files.createLink(link.toPath(), file.toPath());
            } else {
                FileUtils.copyFile(file, link);
            }
        }
    }
}
