package org.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class HBaseUtil {

    public static Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection();
    }


    public static void closeConnection(Connection connection) {
        if (connection != null || !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createTable(Connection connection, String nameSpace, String tableName, String... families) throws IOException {
        //1.获取 admin
        Admin admin = connection.getAdmin();
        //2.创建表格描述
        TableName tableName1 = TableName.valueOf(nameSpace, tableName);
        if (admin.tableExists(tableName1)) {
            System.out.println(tableName + "表已存在!");
            return;
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName1);
        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family));
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        //3.使用 admin 调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("HBase 表创建成功: " + tableName);
        //4.关闭admin
        admin.close();
    }

    public static void dropTable(Connection connection, String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(nameSpace, tableName));
            admin.deleteTable(TableName.valueOf(nameSpace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    public static void putCells(Connection connection, String nameSpace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(columnValue));
            }
        }

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    public static void deleteCells(Connection connection, String nameSpace, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    public static JSONObject getCells(Connection connection, String nameSpace, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        try {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8), new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
        return jsonObject;
    }

    public static JSONObject getAsyncCells(AsyncConnection asyncConnection, String nameSpace, String tableName, String rowKey) throws IOException {
        AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        try {
            Result result = table.get(get).get();
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8), new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    public static AsyncConnection getAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.99.102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncConnection(AsyncConnection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}





























