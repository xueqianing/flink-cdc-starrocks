
/*
 * SocialHub CRM is a customer relationship management application.
 * Copyright (c) 2022-2023 Techsun.Co.Ltd. All rights reserved.
 */
package com.techsun;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Random;

/**
 * @Description class description
 * @Author Xueqian
 * @Date 2023-06-20 14:58
 **/
public class FlinkCdcStarrocks {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        MySqlSource sourceBuilder = MySqlSource.<String>builder()
                .hostname("192.168.1.34")
                .port(3306)
                .databaseList("test")
                .tableList("test.t_customer_group")
                .username("dev")
                .password("dev")
                .deserializer(new MysqlDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> sourceStream = environment.fromSource(sourceBuilder,
                WatermarkStrategy.noWatermarks(), "flink-cdc-source");

        SingleOutputStreamOperator<StarRocksSinkRowDataWithMeta> mapStream = sourceStream.map(new MapFunction<String, StarRocksSinkRowDataWithMeta>() {
            @Override
            public StarRocksSinkRowDataWithMeta map(String record) throws Exception {
                JSONObject jsonRecord = JSON.parseObject(record);
                String database = jsonRecord.getString("database");
                String changeLog = jsonRecord.getString("record");
                String pk = jsonRecord.getString("pk");
                String tableName = jsonRecord.getString("table_name");
                StarRocksSinkRowDataWithMeta rowDataWithMeta = new StarRocksSinkRowDataWithMeta();
                //这里设置下游starrocks 的表名称名称，此示例代码取的是MySQL端的表名称
                rowDataWithMeta.setTable(tableName);
                //这里设置下游starrocks 的库名称，此示例代码取的是MySQL端的库名称
                rowDataWithMeta.setDatabase(database);
                /**
                 * 1.获取MYSQL的binlog日志数据，在MysqlDebeziumDeserializationSchema 类里面添加了一个字段 __op字段
                 * 2.__op = 1 代表是删除操作，__op = 0 代表是插入或者更新操作
                 * 3.下游字段取值， changeLog 这个是一个json结构，下游字段名称和json的key匹配的（字段一样的）会写入数据库，不匹配的字段将自动丢弃。
                 */
                rowDataWithMeta.addDataRow(changeLog);
                return rowDataWithMeta;
            }
        });
        StarRocksSinkOptions sinkBuilder = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url","jdbc:mysql://192.168.1.90:9030")
                .withProperty("load-url", "192.168.1.90:8030")
                /**
                 * 这里的 database-name 和 table-name 只是占位作用，值随便填，没有用处。但是一定要有这两个选项
                 */
                .withProperty("database-name", "database-name")
                .withProperty("table-name", "table-name")
                .withProperty("username", "root")
                .withProperty("password", "root")
                .withProperty("sink.max-retries", "3")
                .withProperty("sink.buffer-flush.max-bytes", "754974720")
                .withProperty("sink.buffer-flush.interval-ms", "3000")
                .withProperty("sink.buffer-flush.max-rows", "300000")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        mapStream.addSink(new StarRocksDynamicSinkFunction<>(sinkBuilder));

        environment.execute();
    }
}
