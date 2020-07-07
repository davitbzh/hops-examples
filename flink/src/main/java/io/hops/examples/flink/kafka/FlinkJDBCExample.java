package io.hops.examples.flink.kafka;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class FlinkJDBCExample {

  public static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(StreamExecutionEnvironment env) {
    List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
    data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
    data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
    data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
    data.add(new Tuple4<>(4, 3L, "Hello world, how are you?", Timestamp.valueOf("1970-01-01 00:00:00.004")));
    data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
    data.add(new Tuple4<>(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
    data.add(new Tuple4<>(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
    data.add(new Tuple4<>(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));
    data.add(new Tuple4<>(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")));
    data.add(new Tuple4<>(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")));
    data.add(new Tuple4<>(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")));
    data.add(new Tuple4<>(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")));
    data.add(new Tuple4<>(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")));
    data.add(new Tuple4<>(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")));
    data.add(new Tuple4<>(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")));
    data.add(new Tuple4<>(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")));
    data.add(new Tuple4<>(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")));
    data.add(new Tuple4<>(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")));
    data.add(new Tuple4<>(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")));
    data.add(new Tuple4<>(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")));
    data.add(new Tuple4<>(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));

    Collections.shuffle(data);
    return env.fromCollection(data);
  }

  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // ------------------------------------------
    String DB_URL = "jdbc:mysql://127.0.0.1:3306/flink_upsert_test";
    String OUTPUT_TABLE1 = "upsertSink";

    String USER_NAME = params.get("mysql_user");
    String PASSWORD = params.get("mysql_passw");
    Integer FLUSH_MAX_ROWS = 1;
    // ------------------------------------------

    Table t = tEnv.fromDataStream(get4TupleDataStream(env).assignTimestampsAndWatermarks(
      new AscendingTimestampExtractor<Tuple4<Integer, Long, String, Timestamp>>() {
        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Long, String, Timestamp> element) {
          return element.f0;
        }}), "id, num, text, ts");

    tEnv.registerTable("T", t);

    String[] fields = {"cnt", "lencnt", "cTag", "ts"};
    tEnv.registerTableSink("upsertSink", JDBCUpsertTableSink.builder()
      .setOptions(JDBCOptions.builder()
        .setDBUrl(DB_URL)
        .setTableName(OUTPUT_TABLE1)
        .build())
      .setTableSchema(TableSchema.builder().fields(
        fields, new DataType[] {BIGINT(), BIGINT(), INT(), TIMESTAMP()}).build())
      .build());

    tEnv.sqlUpdate("CREATE TABLE upsertSink (" +
      "  cnt BIGINT," +
      "  lencnt BIGINT," +
      "  cTag INT," +
      "  ts TIMESTAMP(3)" +
      ") WITH (" +
      "  'connector.type'='jdbc'," +
      "  'connector.url'='" + DB_URL + "'," +
      "  'connector.table'='" + OUTPUT_TABLE1 + "'," +
      "  'connector.username'='" + USER_NAME + "'," +
      "  'connector.username'='" + PASSWORD + "'," +
      "  'connector.write.flush.max-rows'='" + FLUSH_MAX_ROWS + "'" +
      ")");

    env.execute();

  }

}