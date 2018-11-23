package com.lianyi.synchronous.utils;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Stu on 2018/7/12.
 */
public class kafkaUtils {
    private static KafkaProducer kafkaProducer;
    //private static KafkaConsumer consumer;
    private static final Logger logger = LoggerFactory.getLogger(kafkaUtils.class);

    public static void kafkaSentMessage(String bootstrapServers, String topic, String message, String clientId) throws InterruptedException, ExecutionException {
        //        student st=new student("ls",18,"昆明",1);
//        String json = JsonUtil.objectToJson(st);
        //List<student> list = Arrays.asList(new student("ls", 18, "昆明", 1), new student("wangwu", 18, "四川成都", 2), new student("赵六", 13, "浙江温州", 3), new student("haizao", 28, "四川成都成华区", 4));
        //String json = JsonUtil.objectToJson(list);
        //创建一个Properties对象，将要传递给KafkaProducer对象的属性设置在Properties
        Properties properties = new Properties();
        properties.put("client.id", clientId);
        //因为kafka服务器存储的是字节数组，所以需要指定key的序列号器，
        //因为我们打算把键和值定义成字符串类型，所以使用内置的StringSerializer
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //因为kafka服务器存储的是字节数组，所以需要指定key的序列号器
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put("partitioner.class", MyPartition.class.getName());
        //是否获取反馈，0是不获取反馈(消息有可能传输失败)，1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
        //-1是所有in-sync replicas接受到消息时的反馈
        properties.put("request.required.acks", "1");
        //kafka服务器的地址列表，格式是：ip:端口
        properties.put("bootstrap.servers", bootstrapServers);
        logger.debug("开始创建生产者了");
        if (kafkaProducer == null) {
            kafkaProducer = new KafkaProducer(properties);
        }
        ProducerRecord record = new ProducerRecord(topic, message);
        logger.debug("开始发送消息了");
        /*kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //消息发送失败时处理逻辑
            }
        });*/
        kafkaProducer.send(record).get();
        logger.debug("发送消息完毕");
        //new CountDownLatch(1).await();
    }

    public static ArrayList kafkaGetMessage(String bootstrapServers, String clientId, String groupId, String topics) {
        Properties properties = new Properties();
        properties.put("client.id", clientId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        KafkaConsumer   consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topics));
        //每次处理200条消息后才提交
        final int minBatchSize = 2;
        //用于保存消息的list
        ArrayList buffer = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(record.topic());
                    Map data=new HashMap<>();
                    data.put("订阅主题",record.topic());
                    data.put("消息内容",record.value());
                    data.put("消息内容的偏移量",record.offset());
                    data.put("消息内容分区",record.partition());
                    buffer.add(data);
                    System.out.println("value=" + record.value());
                    System.out.println("partition=" + record.partition());
                    System.out.println("offset=" + record.offset());
                }
                //如果读取到的消息满了200条, 就进行处理
                if (buffer.size() >= minBatchSize) {
                    //处理完之后进行提交
                    //异步提交偏移量
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if (e != null) {
                                logger.error("commit fail for offset {}", map, e);
                            }
                        }
                    });

                    return buffer;
                }
            }
        } catch (Exception e) {
            logger.error("订阅消息失败");
            e.printStackTrace();
        } finally {
            consumer.commitSync();
            consumer.close();
            //清除list, 继续接收
            return buffer;
        }
    }
}
