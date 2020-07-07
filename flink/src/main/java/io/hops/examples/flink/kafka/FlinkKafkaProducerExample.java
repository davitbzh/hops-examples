package io.hops.examples.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.FileInputStream;
import java.util.Properties;

public class FlinkKafkaProducerExample {

  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);

    // set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    env.setParallelism(params.getInt("parallelism", 1));
//    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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

    env.addSource(
            new SourceFunction<String>() {
              long value = 0;
              volatile boolean running = true;

              public void run(SourceFunction.SourceContext<String> sourceContext) throws Exception {
                while (running) {
                  sourceContext.collect(String.valueOf(value++));
                  Thread.sleep(100);
                }
              }

              public void cancel() {
                running = false;
              }
            })
        .addSink(
            new FlinkKafkaProducer<String>(
                params.get("topic", "test-topic"), new SimpleStringSchema(), dataKafkaProps));

    env.execute();
  }
}
