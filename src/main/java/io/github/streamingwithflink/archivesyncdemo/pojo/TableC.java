package io.github.streamingwithflink.archivesyncdemo.pojo;

public class TableC {
    private int PC1;
    private String C2;
    private String C3;

    public int getPC1() {
        return PC1;
    }

    public void setPC1(int PC1) {
        this.PC1 = PC1;
    }

    public String getC2() {
        return C2;
    }

    public void setC2(String c2) {
        C2 = c2;
    }

    public String getC3() {
        return C3;
    }

    public void setC3(String c3) {
        C3 = c3;
    }

    @Override
    public String toString() {
        return "TableC{" +
                "PC1=" + PC1 +
                ", C2='" + C2 + '\'' +
                ", C3='" + C3 + '\'' +
                '}';
    }
}
