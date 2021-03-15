package com.yunhai.learn.connector.testConnector;

/**
 * @function: 基于sql的链接器
 * @author: yhzhang
 * @create: 2021-02-02 10:35
 **/
public class HbaseConnectorSQL {
    public String getSQL(){
          String sql="CREATE TABLE hTable (\n" +
                " rowkey INT,\n" +
                " family1 ROW<q1 INT>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'mytable',\n" +
                " 'zookeeper.quorum' = 'localhost:2181'\n" +
                ");";

          return null;
    }
}
