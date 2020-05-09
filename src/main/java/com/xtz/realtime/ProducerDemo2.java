package com.xtz.realtime;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * 订单系统发送数据到kafka。小案例
 */
public class ProducerDemo2 {

    public static void main(String[] args) throws Exception {

        for (long i = 1; i <= 100000000; i++) {


            KafkaProducer<String, String> producer = createProducer();

            JSONObject kooData = createData(i);
            JSONObject order = new JSONObject();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    "koo_trace_to_third", kooData.toJSONString());

            // 这是异步发送的模式
            long startTime = System.currentTimeMillis();
            producer.send(record, new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        // 消息发送成功
                        System.out.println("消息发送成功");
                    } else {
                        // 消息发送失败，需要重新发送
                    }
                }

            });

            long endTime = System.currentTimeMillis();
            if (endTime - startTime > 10) {
                // 你应该走一个监控和报警的过程，开发一些应用程序，系统
                // metric监控和报警，小米开源的open-falcon，挺好用的，监控metric，报警
                // 立马给程序员发短信，或者是发送钉钉，或者发邮件
            }

//            Thread.sleep(10);

            // 这是同步发送的模式
//		producer.send(record).get();
            // 你要一直等待人家后续一系列的步骤都做完，发送消息之后
            // 有了消息的回应返回给你，你这个方法才会退出来
            producer.close();
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();

        // 这里可以配置几台broker即可，他会自动从broker去拉取元数据进行缓存
        props.put("bootstrap.servers", "10.114.41.214:9092,10.114.41.218:9092, 10.114.41.232:9092");
        // 这个就是负责把发送的key从字符串序列化为字节数组
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 这个就是负责把你发送的实际的message从字符串序列化为字节数组
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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

        props.put("max.block.ms", 60000);

        // 创建一个Producer实例：线程资源，跟各个broker建立socket连接资源
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        return producer;

    }

    private static JSONObject createData(Long index) {
        Random random = new Random();
        int traceType = random.nextInt(5);
        JSONObject data = new JSONObject();
        data.put("actionEndTime", System.currentTimeMillis());
        data.put("actionStartTime", System.currentTimeMillis() + 101);
        data.put("appId", "1000002180");

        JSONObject bizData = new JSONObject();
        bizData.put("sourceid", "" + index);
        bizData.put("result", 1);
        bizData.put("mallSku", 1000);
        bizData.put("mallPrice", 100.00);

        data.put("bizData", bizData);
        data.put("clientIp", "182.136.238.251");
        data.put("deviceId", UUID.randomUUID().toString());
        data.put("userId", index);
        data.put("traceId", "trade-loan-start");
        data.put("traceType", traceType);

        String requestData = "{\\\"AppId\\\":\\\"1000002180\\\",\\\"DeviceId\\\":\\\"03aed63e-062f-4f29-ad89-d1723cec6045\\\",\\\"Ip\\\":\\\"182.136.238.251\\\",\\\"UserAgent\\\":\\\"Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/7.0.12(0x17000c27) NetType/WIFI Language/zh_CN\\\",\\\"UserId\\\":1900282787,\\\"platformName\\\":\\\"h5\\\",\\\"productSkuId\\\":29377,\\\"sourceId\\\":\\\"147\\\",\\\"tenantId\\\":\\\"5cf1baf1\\\"}";
        data.put("requestData", requestData);
        String responseData = "{\\\"AppId\\\":\\\"1000003198\\\",\\\"DeviceId\\\":\\\"fd3db581-a293-42c3-ab59-66ce6ab44498\\\",\\\"Ip\\\":\\\"58.243.250.23\\\",\\\"UserAgent\\\":\\\"Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/7.0.10(0x17000a21) NetType/4G Language/zh_CN\\\",\\\"UserId\\\":1900085339,\\\"platformName\\\":\\\"h5\\\",\\\"sourceId\\\":\\\"147\\\",\\\"tenantId\\\":\\\"5cf1baf1\\\"}\",\n" +
                "  \"responseData\": \"{\\\"Content\\\":{\\\"creditLevel\\\":\\\"D3\\\",\\\"defaultLoanPeriod\\\":12,\\\"feeCreditLevel\\\":\\\"B\\\",\\\"firstRepaymentDay\\\":\\\"2020-05-02\\\",\\\"loanAmount\\\":\\\"1651\\\",\\\"loanUseList\\\":[{\\\"loanUseCode\\\":\\\"10001\\\",\\\"loanUseName\\\":\\\"日常消费\\\"},{\\\"loanUseCode\\\":\\\"10002\\\",\\\"loanUseName\\\":\\\"旅游\\\"},{\\\"loanUseCode\\\":\\\"10003\\\",\\\"loanUseName\\\":\\\"装修\\\"},{\\\"loanUseCode\\\":\\\"10004\\\",\\\"loanUseName\\\":\\\"医疗\\\"}],\\\"minAmount\\\":\\\"100\\\",\\\"predictTimer\\\":\\\"\\\",\\\"rates\\\":[{\\\"apr\\\":\\\"21.51%\\\",\\\"fenQiType\\\":0,\\\"handFeeRate\\\":\\\"0\\\",\\\"handFeeTopNum\\\":6,\\\"handIsInstallment\\\":true,\\\"instId\\\":-1,\\\"isPay\\\":false,\\\"loanPeriod\\\":6,\\\"payCutRate\\\":\\\"0\\\",\\\"payFeeTopNum\\\":6,\\\"payIsInstallmentCompatible\\\":true,\\\"payRate\\\":\\\"0\\\",\\\"payType\\\":2,\\\"yearRate\\\":\\\"36.00\\\"},{\\\"apr\\\":\\\"20.78%\\\",\\\"fenQiType\\\":0,\\\"handFeeRate\\\":\\\"0\\\",\\\"handFeeTopNum\\\":9,\\\"handIsInstallment\\\":true,\\\"instId\\\":-1,\\\"isPay\\\":false,\\\"loanPeriod\\\":9,\\\"payCutRate\\\":\\\"0\\\",\\\"payFeeTopNum\\\":9,\\\"payIsInstallmentCompatible\\\":true,\\\"payRate\\\":\\\"0\\\",\\\"payType\\\":2,\\\"yearRate\\\":\\\"36.00\\\"},{\\\"apr\\\":\\\"20.55%\\\",\\\"fenQiType\\\":0,\\\"handFeeRate\\\":\\\"0\\\",\\\"handFeeTopNum\\\":12,\\\"handIsInstallment\\\":true,\\\"instId\\\":-1,\\\"isPay\\\":false,\\\"loanPeriod\\\":12,\\\"payCutRate\\\":\\\"0\\\",\\\"payFeeTopNum\\\":12,\\\"payIsInstallmentCompatible\\\":true,\\\"payRate\\\":\\\"0\\\",\\\"payType\\\":2,\\\"yearRate\\\":\\\"36.00\\\"}],\\\"serverDate\\\":1585795792813},\\\"Result\\\":200}";
        data.put("responseData", responseData);
        //        data.put("userId", index);
        return data;
    }
}

