package com.atguigu.dao;

import com.atguigu.contants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Currency;

public class HBaseDao {
    public static void publishWeibo(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        long ts = System.currentTimeMillis();

        String rowKey = uid + "_" + ts;
        Put contPut = new Put(Bytes.toBytes(rowKey));
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),
                Bytes.toBytes("content"), Bytes.toBytes(content));
        //执行插入操作
        contTable.put(contPut);
        //第二部分：操作微博收件箱表
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        //获取当前发布微博人的fans列族数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relaTable.get(get);
        ArrayList<Put> inboxPuts = new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            //构建微博收件箱表的Put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),
                    Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);
        }
        if (inboxPuts.size() > 0) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPuts);
            inboxTable.close();
        }
        relaTable.close();
        contTable.close();
        connection.close();
    }

    public static void addAttends(String uid, String... attends) throws IOException {
        if (attends.length < 1) {
            System.out.println("请选择关注的人！");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        ArrayList<Put> relaPuts = new ArrayList<>();
        Put uidPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1),
                    Bytes.toBytes(attend), Bytes.toBytes(attend));
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            relaPuts.add(attendPut);
        }
        relaPuts.add(uidPut);
        //执行批量插入操作
        relaTable.put(relaPuts);
        //第二部分：操作收件箱表
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            //获取当前被关注者的近期发布的微博
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contTable.getScanner(scan);

            long ts = System.currentTimeMillis();
            for (Result result : resultScanner) {
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());
            }
        }
        if (!inboxPut.isEmpty()) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }
        relaTable.close();
        contTable.close();
        connection.close();
    }

    //取消关注
    public static void deleteAttends(String uid, String... dels) throws IOException {
        if (dels.length <= 0) {
            System.out.println("请添加用户！");
            return;
        }
        //第一部分 操作用户关系表
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        ArrayList<Delete> relaDeletes = new ArrayList<>();

        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));
            Delete delDelete = new Delete(Bytes.toBytes(del));
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            relaDeletes.add(delDelete);
        }
        relaDeletes.add(uidDelete);
        relaTable.delete(relaDeletes);
        //第二部分 操作收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
        }
        inboxTable.delete(inboxDelete);
        relaTable.close();
        inboxTable.close();
        connection.close();
    }

    //获取初始化页面数据
    public static void getInit(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        //获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        for (Cell cell : result.rawCells()) {
            //构建微博内容表的Get对象
            Get contGet = new Get(CellUtil.cloneValue(cell));
            Result contResult = contTable.get(contGet);
            for (Cell rawCell : contResult.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(rawCell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(rawCell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(rawCell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(rawCell)));
            }
        }
        //关闭资源
        inboxTable.close();
        contTable.close();
        connection.close();
    }

    //获取某个人的所有微博详情
    public static void getWeiBo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取微博内容表对象
        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Scan scan = new Scan();
        //构建过滤器
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + ""));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
        connection.close();
    }


}
