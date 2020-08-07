package io.github.streamingwithflink.archivesyncdemo.pojo;

public class TableB {
    private int PB1;
    private String B2;
    private String B3;

    public int getPB1() {
        return PB1;
    }

    public void setPB1(int PB1) {
        this.PB1 = PB1;
    }

    public String getB2() {
        return B2;
    }

    public void setB2(String b2) {
        B2 = b2;
    }

    public String getB3() {
        return B3;
    }

    public void setB3(String b3) {
        B3 = b3;
    }

    @Override
    public String toString() {
        return "TableB{" +
                "PB1=" + PB1 +
                ", B2='" + B2 + '\'' +
                ", B3='" + B3 + '\'' +
                '}';
    }
}
