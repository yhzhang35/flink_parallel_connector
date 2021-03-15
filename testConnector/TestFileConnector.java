package com.yunhai.learn.connector.testConnector;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @function: 测试并行读取数据
 * @author: yhzhang
 * @create: 2021-03-10 21:12
 **/
public class TestFileConnector {
}

class MySourceFunction extends RichParallelSourceFunction {


    @Override
    public void run(SourceContext ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
