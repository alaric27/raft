package com.yundepot.raft.service;

import com.yundepot.raft.PeerClient;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;


/**
 * @author zhaiyanan
 * @date 2022/7/7  16:27
 */
public class PairServiceTest {

    private PairService pairService;

    @Before
    public void before() {
        PeerClient peerClient = new PeerClient("127.0.0.1", 2727);
//        pairService = peerClient.getRpcClient().create(PairService .class);
    }


    @Test
    public void put() {
        long start = System.currentTimeMillis();
        for (long i = 1; i < 1000; i++) {
            byte[] key = ("aaa" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = getRandomString(10).getBytes(StandardCharsets.UTF_8);
            pairService.put(key, value);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void get() {
        System.out.println(pairService.get("aaa30020".getBytes(StandardCharsets.UTF_8)));
    }

    public static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}