package io.hops.examples.flink.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

import java.util.Objects;
import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataStreams to Tables
 *  - Register a Table under a name
 *  - Run a StreamSQL query on the registered Table
 *
 */
public class StreamSQLExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);
    String planner = params.has("planner") ? params.get("planner") : "flink";

    // set up execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    StreamTableEnvironment tEnv;
    if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
      EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build();
      tEnv = StreamTableEnvironment.create(env, settings);
    } else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
      tEnv = StreamTableEnvironment.create(env);
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
        "example uses flink planner or blink planner.");
      return;
    }

    DataStream<Order> orderA = env.fromCollection(Arrays.asList(
      new Order(1L, "beer", 3),
      new Order(1L, "diaper", 4),
      new Order(3L, "rubber", 2)));

    DataStream<Order> orderB = env.fromCollection(Arrays.asList(
      new Order(2L, "pen", 3),
      new Order(2L, "rubber", 3),
      new Order(4L, "beer", 1)));

    // convert DataStream to Table
    Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");
    // register DataStream as Table
    tEnv.registerDataStream("OrderB", orderB, "user, product, amount");

    // union the two tables
    Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
      "SELECT * FROM OrderB WHERE amount < 2");

    tEnv.toAppendStream(result, Order.class).print();

    env.execute();
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  /**
   * Simple POJO.
   */
  public static class Order {
    public Long user;
    public String product;
    public int amount;

    public Order() {
    }

    public Order(Long user, String product, int amount) {
      this.user = user;
      this.product = product;
      this.amount = amount;
    }

    @Override
    public String toString() {
      return "Order{" +
        "user=" + user +
        ", product='" + product + '\'' +
        ", amount=" + amount +
        '}';
    }
  }
}
