package io.github.streamingwithflink.archivesyncdemo.pojo;

public class TableD {
    private int PD1;
    private String PD2;
    private String PD3;

    public int getPD1() {
        return PD1;
    }

    public void setPD1(int PD1) {
        this.PD1 = PD1;
    }

    public String getPD2() {
        return PD2;
    }

    public void setPD2(String PD2) {
        this.PD2 = PD2;
    }

    public String getPD3() {
        return PD3;
    }

    public void setPD3(String PD3) {
        this.PD3 = PD3;
    }

    @Override
    public String toString() {
        return "TableD{" +
                "PD1=" + PD1 +
                ", PD2='" + PD2 + '\'' +
                ", PD3='" + PD3 + '\'' +
                '}';
    }
}
