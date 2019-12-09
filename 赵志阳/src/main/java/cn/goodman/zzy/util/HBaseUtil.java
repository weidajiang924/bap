package cn.goodman.zzy.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;


public class HBaseUtil {
    private static final Logger logger = Logger.getLogger(HBaseUtil.class);
    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE =
            "hadoop100:2181,hadoop101:2181,hadoop102:2181";
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        //1. 获取连接配置对象
        Configuration configuration = new Configuration();
        //2. 设置连接hbase的参数
        configuration.set(CONNECT_KEY, CONNECT_VALUE);
        //3. 获取Admin对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取Admin对象
     */
//    public static Admin getAdmin() {
//
//        try {
//            admin = connection.getAdmin();
//        } catch (IOException e) {
//            logger.warn("连接HBase的时候异常！", e);
//        }
//        return admin;
//    }

    /**
     * 创建新的命名空间，如果存在就不管
     *
     * @param name
     * @return
     * @throws IOException
     */
    public static boolean createNamespace(String name) throws IOException {
        if (admin.getNamespaceDescriptor(name) != null) {
            return true;
        } else {
            try {
                NamespaceDescriptor descriptor = NamespaceDescriptor.create(name).build();
                admin.createNamespace(descriptor);
                return true;
            } catch (Exception e) {
                return false;
            } finally {
            }
        }
    }


    /**
     * 创建新的表，如果存在删了重建
     *
     * @param tableName
     * @return
     */
    public static boolean createTable(String tableName) {
        try {
            TableName name = TableName.valueOf(tableName);
            if (admin.tableExists(name)) {
                return true;
//                admin.disableTable(name);
//                admin.deleteTable(name);
//
//                // 自己写程序第一次用递归，纪念一下....
//                return createTable(tableName);
            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(name);

                tableDescriptor.addFamily(
                        new HColumnDescriptor("label")
                );
                admin.createTable(tableDescriptor);
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    public static void putData(String tableName, List<String> infos ) {
        try {
            HTable table = (HTable) getTable(tableName);
            table.setAutoFlush(false, false);
            byte[] family = Bytes.toBytes("label");
            byte[] field = Bytes.toBytes("user_label");
            List<Put> puts = new ArrayList<Put>();
            for (String info : infos) {
                String[] words = info.split("\\|");
                Put row = new Put(Bytes.toBytes(words[0]));
                row.addColumn(family, field, Bytes.toBytes(words[1]));
                puts.add(row);
            }
            table.put(puts);
            table.flushCommits();
            closeTable(table);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


    /**
     * 获取表
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table;
    }


    public static void closeAdmin(Admin admin) {
        if (null != admin) {
            try {
                admin.close();
                admin.getConnection().close();
            } catch (IOException e) {
                logger.warn("关闭admin的时候异常!", e);
            }
        }
    }

    public static void closeTable(Table table) throws IOException {
        if (table != null) table.close();
    }

    public static void showResult(Result result) {
        CellScanner cellScanner = result.cellScanner();
        System.out.print("rowKey: " + Bytes.toString(result.getRow()));
        try {
            while (cellScanner.advance()) {
                Cell current = cellScanner.current();
                System.out.print("\t" + new String(CellUtil.cloneFamily(current), "utf-8"));
                System.out.print(" : " + new String(CellUtil.cloneQualifier(current), "utf-8"));
                System.out.print("\t" + new String(CellUtil.cloneValue(current), "utf-8"));
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("判断是否有下一个单元格失败！", e);
        } catch (IOException e) {
            logger.error("克隆数据失败！", e);
        }
    }
}
