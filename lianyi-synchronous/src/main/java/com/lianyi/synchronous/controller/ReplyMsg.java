package com.lianyi.synchronous.controller;

import com.lianyi.synchronous.utils.getIPutils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Stu on 2018/7/31.
 */
@Component
public class ReplyMsg implements Runnable{
    public static final Logger logger= LoggerFactory.getLogger(synchronousController.class);
    @Value("${bootstrap.servers}")
    private String bootstrapservers;
    /**
     * ͬ�������ִ��Ϣ����
     */
    @Override
    public void run(){
        Properties properties = new Properties();
        String clientId = getIPutils.getIP();
        properties.put("client.id", clientId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", bootstrapservers);
        properties.put("group.id", "alibaba");
        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("lianyi"));
        //ÿ�δ���200����Ϣ����ύ
        final int minBatchSize = 2;
        //���ڱ�����Ϣ��list
        ArrayList buffer = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(record.topic());
                    buffer.add("partition="+record.partition());
                    buffer.add("value="+record.value());
                    buffer.add("offset="+record.offset());
                    buffer.add("topic="+record.topic());
                    //ElasticUtils.insertObjByIndex(buffer)
                    System.out.println("value=" + record.value());
                    System.out.println("partition=" + record.partition());
                    System.out.println("offset=" + record.offset());
                }
                if (buffer.size() >= minBatchSize) {
                    //������֮������ύ
                    //�첽�ύƫ����
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if (e != null) {
                                logger.error("commit fail for offset {}", map, e);
                            }
                        }
                    });

                }
            }
        } catch (Exception e) {
            logger.error("������Ϣʧ��");
            e.printStackTrace();
        } finally {
            consumer.commitSync();
            consumer.close();
            //���list, ��������
        }
    }
}
