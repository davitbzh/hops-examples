package io.hops.examples.flink.kafka;

import io.hops.examples.flink.kafka.cli.SqlCommandParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class SqlSubmit {
  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);

    String DB_URL = "jdbc:mysql://127.0.0.1:3306/flink_upsert_test";
    String OUTPUT_TABLE1 = "upsertSink";
    String operaton = params.get("op");

    String USER_NAME = params.get("mysql_user");
    String PASSWORD = params.get("mysql_passw");
    Integer FLUSH_MAX_ROWS = 1;

    SqlSubmit submit = new SqlSubmit(DB_URL, OUTPUT_TABLE1, operaton, USER_NAME, PASSWORD, FLUSH_MAX_ROWS);
    submit.run();
  }

  // --------------------------------------------------------------------------------------------

  private String DB_URL;
  private String OUTPUT_TABLE1;
  private String operaton;
  private String USER_NAME;
  private String PASSWORD;
  private Integer FLUSH_MAX_ROWS;
  private List<String> sql;
  private TableEnvironment tEnv;

  private SqlSubmit(String DB_URL, String OUTPUT_TABLE1, String operaton, String USER_NAME, String PASSWORD,
                    Integer FLUSH_MAX_ROWS) {

    this.DB_URL = DB_URL;
    this.OUTPUT_TABLE1 = OUTPUT_TABLE1;
    this.operaton = operaton;
    this.USER_NAME = USER_NAME;
    this.PASSWORD = PASSWORD;
    this.FLUSH_MAX_ROWS = FLUSH_MAX_ROWS;
    this.sql = new ArrayList<String>();

  }

  private void run() throws Exception {

    EnvironmentSettings settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();

     this.tEnv = TableEnvironment.create(settings);

    if (this.operaton.equals("create")){
      this.sql = java.util.Arrays.asList(
        "CREATE TABLE upsertSink (" +
          "  cnt BIGINT," +
          "  lencnt BIGINT," +
          "  cTag INT," +
          "  ts TIMESTAMP(3)" +
          ") WITH (" +
          "  'connector.type'='jdbc'," +
          "  'connector.url'='" + this.DB_URL + "'," +
          "  'connector.table'='" + this.OUTPUT_TABLE1 + "'," +
          "  'connector.username'='" + this.USER_NAME + "'," +
          "  'connector.username'='" + this.PASSWORD + "'," +
          "  'connector.write.flush.max-rows'='" + this.FLUSH_MAX_ROWS + "'" +
          ")",

        "INSERT INTO upsertSink SELECT id, num, ts FROM T"
      );
    } else if (this.operaton.equals("upsert")){
      this.sql = java.util.Arrays.asList(
        "CREATE TABLE upsertSink (" +
          "  cnt BIGINT," +
          "  lencnt BIGINT," +
          "  cTag INT," +
          "  ts TIMESTAMP(3)" +
          ") WITH (" +
          "  'connector.type'='jdbc'," +
          "  'connector.url'='" + this.DB_URL + "'," +
          "  'connector.table'='" + this.OUTPUT_TABLE1 + "'," +
          "  'connector.username'='" + this.USER_NAME + "'," +
          "  'connector.username'='" + this.PASSWORD + "'," +
          "  'connector.write.flush.max-rows'='" + this.FLUSH_MAX_ROWS + "'" +
          ")",

        "INSERT INTO upsertSink SELECT id, num, ts FROM T"
      );
    }

    List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(this.sql);
    for (SqlCommandParser.SqlCommandCall call : calls) {
      callCommand(call);
    }
    tEnv.execute("SQL Job");
  }

  // --------------------------------------------------------------------------------------------

  private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
    switch (cmdCall.command) {
      case SET:
        callSet(cmdCall);
        break;
      case CREATE_TABLE:
        callCreateTable(cmdCall);
        break;
      case INSERT_INTO:
        callInsertInto(cmdCall);
        break;
      default:
        throw new RuntimeException("Unsupported command: " + cmdCall.command);
    }
  }

  private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
    String key = cmdCall.operands[0];
    String value = cmdCall.operands[1];
    tEnv.getConfig().getConfiguration().setString(key, value);
  }

  private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
    String ddl = cmdCall.operands[0];
    try {
      tEnv.sqlUpdate(ddl);
    } catch (SqlParserException e) {
      throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
    }
  }

  private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
    String dml = cmdCall.operands[0];
    try {
      tEnv.sqlUpdate(dml);
    } catch (SqlParserException e) {
      throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
    }
  }

}
