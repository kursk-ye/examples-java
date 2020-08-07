package io.github.streamingwithflink.archivesyncdemo.pojo;

public class OldTableA {
    private int PA1;
    private String A2;
    private String FA3;
    private String FA4;

    public int getPA1() {
        return PA1;
    }

    public void setPA1(int PA1) {
        this.PA1 = PA1;
    }

    public String getA2() {
        return A2;
    }

    public void setA2(String a2) {
        A2 = a2;
    }

    public String getFA3() {
        return FA3;
    }

    public void setFA3(String FA3) {
        this.FA3 = FA3;
    }

    public String getFA4() {
        return FA4;
    }

    public void setFA4(String FA4) {
        this.FA4 = FA4;
    }

    @Override
    public String toString() {
        return "OldTableA{" +
                "PA1=" + PA1 +
                ", A2='" + A2 + '\'' +
                ", FA3='" + FA3 + '\'' +
                ", FA4='" + FA4 + '\'' +
                '}';
    }
}
