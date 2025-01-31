package com.xtz.realtime;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        // 这里可以配置几台broker即可，他会自动从broker去拉取元数据进行缓存
        props.put("bootstrap.servers", "10.114.41.214:9092,10.114.41.218:9092, 10.114.41.232:9092");
//        props.put("bootstrap.servers", "hadoop1:9092,hadoop2:9092, hadoop3:9092");
//        props.put("bootstrap.servers", "localhost:9092");
        // 这个就是负责把发送的key从字符串序列化为字节数组
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 这个就是负责把你发送的实际的message从字符串序列化为字节数组
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        props.put("partitioner.class", "com.ppdai.realtime.HotDataPartitioner");

        props.put("acks", "-1");

        props.put("retries", 3);

        //默认值是：16384，就是16kb
        props.put("batch.size", 131072);//这里设置了128kb

        //linger.ms，这个值默认是0，意思就是消息必须立即被发送，但是这是不对的，一般设置一个100毫秒之类的，这样的话就是说，这个消息被发送出去后进入一个batch，如果100毫秒内，这个batch满了16kb，自然就会发送出去
        props.put("linger.ms", 100);

        //buffer.memory：设置发送消息的缓冲区，默认值是33554432，就是32MB
        props.put("buffer.memory", 33554432);

        //使用lz4压缩
        props.put("compression.type", "lz4");

        //这个参数用来控制发送出去的消息的大小，默认是1048576字节，也就1mb
        props.put("max.request.size", 10485760);

        //这个就是说发送一个请求出去之后，他有一个超时的时间限制，默认是30秒，如果30秒都收不到响应，那么就会认为异常，会抛出一个TimeoutException来让我们进行处理
//        props.put("request.timeout.ms", 30);

//        props.put("max.block.ms", 3000);

        // 创建一个Producer实例：线程资源，跟各个broker建立socket连接资源
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        String meaasge = "lyn111";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                "xtz", "xtz", meaasge);


        // 这是异步发送的模式
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    // 消息发送成功
                    System.out.println("消息发送成功");
                } else {
                    // 消息发送失败，需要重新发送
                    System.out.println("消息发送失败啦...." + exception);
                }
            }

        });

        Thread.sleep(10 * 1000);

        // 这是同步发送的模式
//		producer.send(record).get();
        // 你要一直等待人家后续一系列的步骤都做完，发送消息之后
        // 有了消息的回应返回给你，你这个方法才会退出来
        producer.close();
    }


}

