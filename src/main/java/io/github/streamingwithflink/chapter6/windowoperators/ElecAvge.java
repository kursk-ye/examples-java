package io.github.streamingwithflink.chapter6.windowoperators;

public class ElecAvge {
    private  double avge = 0.0;
    private String mid = "";
    private int size = 0;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ElecAvge(String id, double avge, int size) {
        this.avge = avge;
        this.mid = id;
        this.size = size;
    }


    public double getAvge() {
        return avge;
    }

    public void setAvge(double avge) {
        this.avge = avge;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    @Override
    public String toString(){
        return "id: " + this.getMid() + " avge: " + this.getAvge() + " size : " + this.getSize();
    }


}
