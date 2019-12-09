package com.xuebotao.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.io.IOException;


public class HBaseUtils {

    private static final Logger logger = Logger.getLogger(HBaseUtils.class);

    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hbase2:2181,hbase3:2181,hbase4:2181";
    private static Connection connection;

    static {
        //1. 获取连接配置对象
        Configuration configuration = HBaseConfiguration.create();
        //2. 设置连接hbase的参数
        configuration.set(CONNECT_KEY, CONNECT_VALUE);
        //3. 获取connection对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.warn("连接HBase的时候异常！", e);
        }
    }

    /**
     * 获取Admin对象
     */
    public static Admin getAdmin() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.warn("连接HBase的时候异常！", e);
        }
        return admin;
    }
    public static void close(Admin admin) {
        if(null != admin) {
            try {
                admin.close();
                admin.getConnection().close();
            } catch (IOException e) {
                logger.warn("关闭admin的时候异常!", e);
            }
        }
    }
    public static Table getTable(String tablename) {
        Table table = null;
        if(StringUtils.isNotEmpty(tablename)) {
            try {
                table = connection.getTable(TableName.valueOf(tablename));
            } catch (IOException e) {
                logger.warn("获取表产生异常！", e);
            }
        }
        return table;
    }

    public static void close(Table table) {
        if(table != null) {
            try {
                table.close();
            } catch (IOException e) {
                logger.warn("关闭table的时候产生异常！", e);
            }
        }
    }
}
