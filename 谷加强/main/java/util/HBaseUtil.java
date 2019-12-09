package util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HBaseUtil {
    private static final Logger logger = Logger.getLogger(HBaseUtil.class);

    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hadoop05:2181,hadoop06:2181,hadoop07:2181";
    private static Connection connection = null;
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

    /**
     * 关闭admin对象
     * @param admin
     */
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

    /**
     * 获取表连接
     * @return
     */
    public static Table getTable(String tableName) {
        Table table = null;
        try {
            if(StringUtils.isEmpty(tableName)){
                tableName = "userTags:20191209";
            }
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void createTable(String tableName) {
        try {
            Admin admin = getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("tags");
            columnDescriptor.setVersions(1, 5);
            columnDescriptor.setTimeToLive(24*60*60);
            columnDescriptor.setBlockCacheEnabled(true);
            tableDescriptor.addFamily(columnDescriptor);
            tableDescriptor.setDurability(Durability.ASYNC_WAL);
            admin.createTable(tableDescriptor);
            close(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭admin对象
     * @param table
     */
    public static void closeTable(Table table) {
        if(null != table) {
            try {
                table.close();
            } catch (IOException e) {
                logger.warn("关闭table的时候异常!", e);
            }
        }
    }
}
