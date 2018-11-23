package com.lianyi.synchronous.controller;

import com.alibaba.fastjson.JSONObject;
import com.lianyi.service.EsOperationService;
import com.lianyi.synchronous.utils.ResponseResult;
import com.lianyi.synchronous.utils.getIPutils;
import com.lianyi.synchronous.utils.kafkaUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Created by Stu on 2018/7/26.
 */
@Controller
@RequestMapping(value = "/synchronous/v1/message")
public class synchronousController {
    @Value("${bootstrap.servers}")
    private String bootstrapservers;
    private static final Logger LOGGER = LoggerFactory.getLogger(synchronousController.class);
    @Autowired
    private ReplyMsg replyMsg;
    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;
   /* @Autowired
    private EsOperationService esOperationService;*/

    @RequestMapping(value = "/send",method = RequestMethod.POST)
    @ResponseBody
    public ResponseResult kafkaSendMessage(String topic, String message) {
        ResponseResult result = new ResponseResult();
        String ip = getIPutils.getIP();
        try {
            kafkaUtils.kafkaSentMessage(bootstrapservers, topic, message, ip);
            result.setMsg("成功");
            result.setCode(200);
            result.setData("");
        } catch (Exception e) {
            LOGGER.error("出异常了，" + e.getMessage());
            e.printStackTrace();
            result.setMsg("失败");
            result.setCode(500);
            result.setData("");
        }
        return result;
    }
    @RequestMapping(value = "/get",method = RequestMethod.GET,produces = MediaType.APPLICATION_JSON_VALUE + ";charset=utf-8")
    @ResponseBody
    public ResponseResult kafkaGetMessage(String groupId, String topics) {
        ResponseResult result = new ResponseResult();
        String ip = getIPutils.getIP();
        try {
            ArrayList message = kafkaUtils.kafkaGetMessage(bootstrapservers, ip, groupId, topics);
            result.setMsg("成功");
            result.setCode(200);
            result.setData(message);
        } catch (Exception e) {
            e.printStackTrace();
            result.setMsg("失败");
            result.setCode(500);
            result.setData("");
        }
        return result;
    }

    /**
     * 异步处理回执消息
     */
    @PostConstruct
    public void replyMsg(){
        //Thread t=new Thread(replyMsg);
        //t.start();
        taskExecutor.execute(replyMsg);
    }


}
