//package io.hops.examples.flink.kafka;
//
//import org.apache.flink.api.java.io.jdbc.JDBCOptions;
//import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableSchema;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.types.DataType;
//import static org.apache.flink.table.api.DataTypes.BIGINT;
//import static org.apache.flink.table.api.DataTypes.INT;
//import static org.apache.flink.table.api.DataTypes.STRING;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.sql.Timestamp;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//import org.apache.flink.table.functions.TemporalTableFunction;
//
//public class TemporalJoins {
//
//  public static void main(String[] args) throws Exception {
//
//// Get the strea
// m and table environments.
////    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////    StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);
//
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.getConfig().enableObjectReuse();
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//
//// Provide a sample static data set of the rates history table.
//    List <Tuple2<String, Long>>ratesHistoryData =new ArrayList<>();
//    ratesHistoryData.add(Tuple2.of("USD", 102L));
//    ratesHistoryData.add(Tuple2.of("EUR", 114L));
//    ratesHistoryData.add(Tuple2.of("YEN", 1L));
//    ratesHistoryData.add(Tuple2.of("EUR", 116L));
//    ratesHistoryData.add(Tuple2.of("USD", 105L));
//// Create and register an example table using the sample data set.
//    DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
//    Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "r_currency, r_rate, r_proctime.proctime");
//    tEnv.registerTable("RatesHistory", ratesHistory);
//// Create and register the temporal table function "rates".
//// Define "r_proctime" as the versioning field and "r_currency" as the primary key.
//    TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency");
//    tEnv.registerFunction("Rates", rates);
//
//    List <Tuple2<Long, String>> ordersData =new ArrayList<>();
//    ordersData.add(Tuple2.of(((2L, "Euro"));
//    ordersData.add(Tuple2.of(((1L, "US Dollar"));
//    ordersData.add(Tuple2.of(((50L, "Yen"));
//    ordersData.add(Tuple2.of(((3L, "Euro"));
//    ordersData.add(Tuple2.of(((5L, "US Dollar"));
//
//    DataStream<Tuple2<Long, String>> ordersDataStream = env.fromCollection(ordersData);
//    Table orders = tEnv.fromDataStream(ordersDataStream, "o_id, o_currency, r_proctime.proctime");
//    tEnv.registerTable("Orders", orders);
//
//    tEnv.registerFunction(
//      "Rates",
//      ratesHistory.createTemporalTableFunction("proctime", "currency"));
//
//
//  }
//}
