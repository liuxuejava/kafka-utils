package com.lianyi.synchronous.utils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Stu on 2018/7/26.
 */
public class getIPutils {
    private static final Logger log = LoggerFactory.getLogger(getIPutils.class);

    /**
     * ��ȡ����IP���ߺ���
     * @return
     */
    @Test
    public static String getIP() {
        InetAddress addr = null;
        String ip = null;
        try {
            addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress().toString(); //��ȡ����ip
            String hostName = addr.getHostName().toString(); //��ȡ�������������
            System.out.println(ip);
            System.out.println(hostName);

        } catch (UnknownHostException e) {
            e.printStackTrace();
            log.error("���쳣�ˣ�" + e.getMessage());
        }
        return ip;

    }
}
