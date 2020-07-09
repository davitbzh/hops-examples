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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;


public class FlinkJDBCExample2 {

  public static DataStream<Tuple3<Integer, Integer, String>> get3TupleDataStream1(StreamExecutionEnvironment env) {
    List<Tuple3<Integer, Integer, String>> data = new ArrayList<>();
    data.add(new Tuple3<>(1, 1, "Hi"));
    data.add(new Tuple3<>(2, 2, "Hello"));
    data.add(new Tuple3<>(3, 3, "Hello world"));
    data.add(new Tuple3<>(4, 4, "Hello world, how are you?"));

    Collections.shuffle(data);
    return env.fromCollection(data);
  }

  public static DataStream<Tuple3<Integer, Integer, String>> get3TupleDataStream2(StreamExecutionEnvironment env) {

    List<Tuple3<Integer, Integer, String>> data = new ArrayList<>();
    data.add(new Tuple3<>(1, 1, "Hi"));
    data.add(new Tuple3<>(2, 2, "Hello"));
    data.add(new Tuple3<>(3, 3, "Hello world from Sweden"));
    data.add(new Tuple3<>(4, 4, "Hello world, how are you?, Sweden is fine :) "));

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

    if (operaton.equals("stream1")){

      Table t = tEnv.fromDataStream(get3TupleDataStream1(env).assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<Tuple3<Integer, Integer, String>>() {
          @Override
          public long extractAscendingTimestamp(Tuple3<Integer, Integer, String> element) {
            return element.f0;
          }}), "incrNum, id, text");

      tEnv.registerTable("T", t);

    } else if (operaton.equals("stream2")){

      Table t = tEnv.fromDataStream(get3TupleDataStream2(env).assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<Tuple3<Integer, Integer, String>>() {
          @Override
          public long extractAscendingTimestamp(Tuple3<Integer, Integer, String> element) {
            return element.f0;
          }}), "incrNum, id, text");

      tEnv.registerTable("T", t);

    }

    String[] fields = {"id", "text"};
    String[] keys = {"id"};

    JDBCUpsertTableSink tablesink = JDBCUpsertTableSink.builder()
      .setOptions(JDBCOptions.builder()
        .setDBUrl(DB_URL)
        .setTableName(OUTPUT_TABLE1)
        .setUsername(USER_NAME)
        .setPassword(PASSWORD)
        .build()
      )
      .setTableSchema(TableSchema.builder().fields(
        fields, new DataType[] {INT(), STRING()}).build())
      .build();

    tablesink.setKeyFields(keys);
    tablesink.setIsAppendOnly(false);
    tEnv.registerTableSink("upsertSink", tablesink);


    tEnv.sqlUpdate("INSERT INTO upsertSink SELECT id, text FROM T GROUP BY id, text");

    tEnv.execute("SQL JOB");
//    env.execute();

  }

}
