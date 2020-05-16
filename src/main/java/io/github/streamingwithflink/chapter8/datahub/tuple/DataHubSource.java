package io.github.streamingwithflink.chapter8.datahub.tuple;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public abstract class DataHubSource<OUT> extends RichParallelSourceFunction<OUT> {

}
