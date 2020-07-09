package io.hops.examples.flink.kafka;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple3;
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
import static org.apache.flink.table.api.DataTypes.STRING;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class FlinkJDBCExample {

  public static DataStream<Tuple3<Integer, Long, String>> get3TupleDataStream(StreamExecutionEnvironment env) {
    List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
    data.add(new Tuple3<>(1, 1L, "Hi"));
    data.add(new Tuple3<>(2, 2L, "Hello"));
    data.add(new Tuple3<>(3, 2L, "Hello world"));
    data.add(new Tuple3<>(4, 3L, "Hello world, how are you?"));
    data.add(new Tuple3<>(5, 3L, "I am fine."));
    data.add(new Tuple3<>(6, 3L, "Luke Skywalker"));
    data.add(new Tuple3<>(7, 4L, "Comment#1"));
    data.add(new Tuple3<>(8, 4L, "Comment#2"));
    data.add(new Tuple3<>(9, 4L, "Comment#3"));
    data.add(new Tuple3<>(10, 4L, "Comment#4"));
    data.add(new Tuple3<>(11, 5L, "Comment#5"));
    data.add(new Tuple3<>(12, 5L, "Comment#6"));
    data.add(new Tuple3<>(13, 5L, "Comment#7"));
    data.add(new Tuple3<>(14, 5L, "Comment#8"));
    data.add(new Tuple3<>(15, 5L, "Comment#9"));
    data.add(new Tuple3<>(16, 6L, "Comment#10"));
    data.add(new Tuple3<>(17, 6L, "Comment#11"));
    data.add(new Tuple3<>(18, 6L, "Comment#12"));
    data.add(new Tuple3<>(19, 6L, "Comment#13"));
    data.add(new Tuple3<>(20, 6L, "Comment#14"));
    data.add(new Tuple3<>(21, 6L, "Comment#15"));
    data.add(new Tuple3<>(22, 2L, "Hello"));
    data.add(new Tuple3<>(23, 2L, "Hello world"));
    data.add(new Tuple3<>(24, 2L, "Hello world"));
    data.add(new Tuple3<>(25, 2L, "Hello world"));

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
    String operaton = params.get("op");

    String USER_NAME = params.get("mysql_user");
    String PASSWORD = params.get("mysql_passw");
    Integer FLUSH_MAX_ROWS = 1;
    // ------------------------------------------

    if (operaton.equals("create")){

//      tEnv.registerTableSink("upsertSink", JDBCUpsertTableSink.builder()
//        .setOptions(JDBCOptions.builder()
//          .setDBUrl(DB_URL)
//          .setTableName(OUTPUT_TABLE1)
//          .build())
//        .setTableSchema(TableSchema.builder().fields(
//          fields, new DataType[] {BIGINT(), BIGINT(), INT(), TIMESTAMP(3)}).build())
//        .build());

      tEnv.sqlUpdate("CREATE TABLE upsertSink (" +
        "  cnt INT ," +
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

      // create database flink_upsert_test;
      //CREATE TABLE upsertSink (cnt INT NOT NULL DEFAULT 0,lencnt BIGINT NOT NULL DEFAULT 0,cTag INT NOT NULL DEFAULT 0, PRIMARY KEY (cTag))ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

    } else if (operaton.equals("upsert")){

      Table t = tEnv.fromDataStream(get3TupleDataStream(env).assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<Tuple3<Integer, Long, String>>() {
          @Override
          public long extractAscendingTimestamp(Tuple3<Integer, Long, String> element) {
            return element.f0;
          }}), "id, num, text");

      tEnv.registerTable("T", t);

      String[] fields = {"cnt", "lencnt", "cTag"};

      tEnv.registerTableSink("upsertSink", JDBCUpsertTableSink.builder()
        .setOptions(JDBCOptions.builder()
          .setDBUrl(DB_URL)
          .setTableName(OUTPUT_TABLE1)
          .setUsername(USER_NAME)
          .setPassword(PASSWORD)
          .build())
        .setTableSchema(TableSchema.builder().fields(
          fields, new DataType[] {BIGINT(), BIGINT(), BIGINT()}).build())
        .build());

//      tEnv.sqlUpdate("INSERT INTO upsertSink SELECT cnt, lencnt, cTag FROM T");


      tEnv.sqlUpdate("INSERT INTO upsertSink SELECT cnt, COUNT(len) AS lencnt, cTag FROM" +
                        " (SELECT len, COUNT(cTag) as cnt, cTag FROM" +
                        " (SELECT id, CHAR_LENGTH(text) AS len, num AS cTag FROM T)" +
                        " GROUP BY len, cTag)" +
                        " GROUP BY cnt, cTag");

    }

    tEnv.execute("SQL JOB");
//    env.execute();

  }

}
