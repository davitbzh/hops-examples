package io.hops.examples.flink.kafka;

// import io.hops.util.Hops;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.FileInputStream;
import java.util.Properties;

public class FlinkKafkaConsumerExample {

  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);

    // set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    env.setParallelism(params.getInt("parallelism", 1));

    // --------------------------------------------------------------------------------------------------------------
    // configure Kafka consumer
    // Data
    Properties dataKafkaProps = new Properties();

    // Read password from local file
    String materialPassword;
    try (FileInputStream fis = new FileInputStream("material_passwd")) {
      StringBuilder sb = new StringBuilder();
      int content;
      while ((content = fis.read()) != -1) {
        sb.append((char) content);
      }
      materialPassword = sb.toString();
    }

    dataKafkaProps.setProperty("bootstrap.servers", params.get("bootstrap_servers"));
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPassword);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPassword);
    dataKafkaProps.setProperty("ssl.key.password", materialPassword);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");
    dataKafkaProps.setProperty("group.id", "something");

    // --------------------------------------------------------------------------------------------------------------

    // Construct a workflow, read data from Kafka, and print the data in a new line.
    DataStream<String> messageStream =
        env.addSource(
            new FlinkKafkaConsumer<>(
                params.get("topic", "test-topic"), new SimpleStringSchema(), dataKafkaProps));

    messageStream
        .rebalance()
        .map(
            new MapFunction<String, String>() {
              @Override
              public String map(String s) throws Exception {
                return "Flink says " + s + System.getProperty("line.separator");
              }
            })
        .print();
    // Invoke execute to trigger the execution.
    env.execute();
  }
}
