package io.github.streamingwithflink.chapter8;

import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;
import java.io.Serializable;

public class CommoncsvInputFormat extends CsvInputFormat {

    public CommoncsvInputFormat(Path filePath){
        super(filePath);
    }

    @Override
    protected Object fillRecord(Object reuse, Object[] parsedValues) {
        return null;
    }

    @Override
    public void reopen(InputSplit split, Serializable state) throws IOException {

    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit split) throws IOException {

    }
}
