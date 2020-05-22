package io.github.streamingwithflink.chapter8.drds;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlSourceFunction extends RichSourceFunction<ElecMeterReading> {
  private Boolean running = true;
  private MysqlClient dbclient;
  private ResultSet resultSet;
  private int resultSetSize = 0;
  private int resultSetCount = 0;
  // source function can not use state ?
  // private ValueStateDescriptor<Integer> descResultSetSize;
  // private ValueStateDescriptor<Integer> descResultSetCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    dbclient = new MysqlClient();
    resultSet = dbclient.readDB();

    /*
    descResultSetSize = new ValueStateDescriptor<Integer>("resultSetSize" , Types.INT);
    descResultSetCount = new ValueStateDescriptor<Integer>("resultSetCount" , Types.INT);
    */
  }

  @Override
  public void run(SourceContext<ElecMeterReading> ctx) throws Exception {
    /*
    resultSetSize = getRuntimeContext().getState(descResultSetSize);
    resultSetSize.update(resultSet.getRow());
    */

    resultSetSize = resultSet.getRow();

    while (running) {

      while (resultSet.next()) {
        String id = resultSet.getString("id");
        Long ts = (resultSet.getTimestamp("mid")).getTime();
        Double value = resultSet.getDouble("dayelecvalue");

        ctx.collect(
            new ElecMeterReading() {
              {
                setId(id);
                setTimestamp(ts);
                setDayElecValue(value);
              }
            });

        resultSetCount++;

        if ( resultSetCount >= resultSetSize) { //
          cancel();
        }
      }
    }

    try {
      resultSet.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
