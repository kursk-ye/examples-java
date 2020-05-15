package io.github.streamingwithflink.chapter7.querystate;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

// 虽然程序可以运行，但不知道是不是JobId的问题，查不到对应的state

public class ClientDemo {
    public static void main(String[] args) throws UnknownHostException {
        QueryableStateClient client = new QueryableStateClient("127.0.0.1", 9076);

        ValueStateDescriptor<Tuple2<String,Double>> descriptor =
                new ValueStateDescriptor<Tuple2<String, Double>>("sum",
                        TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                        }));

        CompletableFuture<ValueState<Tuple2<String,Double>>> resultFuture =
                client.getKvState(JobID.fromHexString("a64c8c37ee88b510c0b2749e9bdeb16e"),
                        "query-name",
                        "MID-0",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        descriptor);

        resultFuture.thenAccept( response -> {
            try{
                Tuple2<String,Double> rs = response.value();
                System.out.println(rs.f0 + " sum is " + rs.f1);
            }catch (Exception e){
                e.printStackTrace();
            }

        });


    }
}
