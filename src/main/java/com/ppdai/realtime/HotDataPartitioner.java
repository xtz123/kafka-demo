package com.ppdai.realtime;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @ClassName HotDataPartitioner
 * @Description 自定义分区。
 * @Author xutengzhong
 * @Date 2020/2/17 2:00
 **/
public class HotDataPartitioner implements Partitioner {

    /**
     * 别人使用分区器的时候调用: props.put("partitioner.class", "com.ppdai.realtime.HotDataPartitioner");
     */

    private Random random;

    @Override
    public int partition(String topic, Object keyObj, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String key = (String) keyObj;
        List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic);
        int partitionCount = partitionInfoList.size();
        int hotDataPartition = partitionCount - 1;
        return !key.contains("hot_data") ? random.nextInt(partitionCount - 1) : hotDataPartition;
    }


    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        random = new Random();
    }
}
