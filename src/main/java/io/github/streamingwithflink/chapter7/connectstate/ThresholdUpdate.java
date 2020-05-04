package io.github.streamingwithflink.chapter7.connectstate;

public class ThresholdUpdate {
    public String id;
    public Double threshold;

    public ThresholdUpdate(String id ,Double threshold){
        this.id = id;
        this.threshold = threshold;
    }
}
