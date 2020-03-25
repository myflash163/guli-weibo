package com.atguigu.utils;

import com.atguigu.contants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseUtil {
    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();
    }

    private static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return exists;
    }

    public static void createTable(String tableName, int versions, String... cfs) throws IOException {

        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！！！");
            return;
        }
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！");
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }
}
