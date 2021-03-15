package com.yunhai.learn.connector;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @function: Hbase的原生链接
 * @author: yhzhang
 * @create: 2021-02-02 09:29
 **/
public class HbaseConnector implements SourceFunction<Result> {
    //定义链接
    Connection connection;
    Table table;


    public void run(SourceContext ctx) throws Exception {
        try {
            // 加载HBase的配置
            Configuration configuration = HBaseConfiguration.create();

            // 读取配置文件
            configuration.set("hbase.zookeeper.quorum", "152.136.221.59");
            configuration.set("hbase.zookeeper.property.clientPort", "2182");
            configuration.setInt("hbase.rpc.timeout", 30000);
            configuration.setInt("hbase.client.operation.timeout", 30000);
            configuration.setInt("hbase.client.scanner.timeout.period", 30000);
            configuration.set("zookeeper.znode.parent","/hbase");
            configuration.set("hbase.master","152.136.221.59:60011");
             connection = ConnectionFactory.createConnection(configuration);

            HBaseAdmin hbaseadmin = new HBaseAdmin(connection);

            TableName tableName = TableName.valueOf("GROUND_WELLS");

            // 获取表对象
            table = connection.getTable(tableName);

            System.out.println("########"+hbaseadmin.tableExists(tableName)+">>>>>>>>");

            System.out.println("[HbaseSink] : open HbaseSink finished");
        } catch (Exception e) {
            System.out.println(e);
        }

        //开始读取数据
        Scan scan=new Scan();
        for (Result result:table.getScanner(scan)){
            ctx.collect(result);
        }

    }


    //关闭流
    public void cancel() {
        try {
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
