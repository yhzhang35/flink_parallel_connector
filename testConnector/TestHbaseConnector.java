package com.yunhai.learn.connector.testConnector;

import com.yunhai.learn.connector.HbaseConnector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.Result;

/**
 * @function: 测试hbase
 * @author: yhzhang
 * @create: 2021-02-02 09:50
 **/
public class TestHbaseConnector {
    public static void main (String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Result> source= env.addSource(new HbaseConnector());
        source.print();
        env.execute("hbase connect native");
    }
}
