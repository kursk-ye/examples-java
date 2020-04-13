package io.github.streamingwithflink.chapter5.util;

import org.apache.flink.shaded.guava18.com.google.common.collect.BoundType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        int Maxparallelism = env.getMaxParallelism();

        System.out.println("parallelism: " + parallelism);
        System.out.println("Maxparallelism: " + Maxparallelism);
    }
}
