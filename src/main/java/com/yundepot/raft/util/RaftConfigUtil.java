package com.yundepot.raft.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yundepot.oaa.util.StringUtils;
import com.yundepot.raft.config.RaftConfig;
import com.yundepot.raft.exception.RaftException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author zhaiyanan
 * @date 2019/6/21 14:53
 */
public class RaftConfigUtil {

    public static Properties getProperties(String configFile) {
        if (StringUtils.isNotBlank(configFile)) {
            return getPropertiesByConfig(configFile);
        } else {
            return getProperties();
        }
    }

    public static Properties getPropertiesByConfig(String configFile) {
      Properties properties = new Properties();
      try(BufferedReader bufferedReader = new BufferedReader(new FileReader(configFile))) {
          properties.load(bufferedReader);
      } catch (Exception e) {
          throw new RaftException("加载配置文件异常");
      }
      return properties;
    }


    public static Properties getProperties() {
        Properties properties = new Properties();
        try(InputStream inputStream = RaftConfigUtil.class.getClassLoader().getResourceAsStream("config/raft.properties")) {
            properties.load(inputStream);
        } catch (Exception e) {
            throw new RaftException("加载配置文件异常");
        }
        return properties;
    }

    /**
     * 获取raft配置
     * @param configFile
     * @return
     */
    public static RaftConfig getRaftConfig(String configFile) {
        Properties properties = getProperties(configFile);
        RaftConfig raftConfig = JSON.parseObject(JSON.toJSONString(properties), new TypeReference<RaftConfig>(){}.getType());
        return raftConfig;
    }
}
