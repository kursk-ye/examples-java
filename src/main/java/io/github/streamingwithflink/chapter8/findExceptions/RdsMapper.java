package io.github.streamingwithflink.chapter8.findExceptions;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import io.github.streamingwithflink.chapter8.drds.MysqlClient;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.ResultSet;

public class RdsMapper extends RichFlatMapFunction<PoJoElecMeterReading,String> {
    MysqlClient RdsClient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        RdsClient = new MysqlClient();
    }

    @Override
    public void flatMap(PoJoElecMeterReading value, Collector<String> out) throws Exception {
        String datahubId = value.getId();
        ResultSet res = RdsClient.readDB(datahubId);

        if (res.next()){
            out.collect("recorder  in rds , then update ");
            // execute rds update
        }else{
            out.collect(" no recorder in rds ,then insert ");
            RdsClient.writeDB(value);
        }
    }

    @Override
    public void close() throws Exception {
        RdsClient.close();
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
    }


}
