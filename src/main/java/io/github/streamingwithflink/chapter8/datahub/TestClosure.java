package io.github.streamingwithflink.chapter8.datahub;

import java.util.ArrayList;
import java.util.List;

public class TestClosure {
    public static void main(String[] args) {
        List<OutCount> list = new ArrayList<>();
        OutCount oct = new OutCount();

        for(int j=0; j<100 ; j++){
            int finalJ = j;
            oct.setCnt(new Count(j));
            list.add(oct);
        }

        for(OutCount cn : list){
            System.out.println(cn.getCnt().getC());
        }
    }
}

class Count{
    int c;

    Count(int c){
        this.c = c;
    }

    public int getC() {
        return c;
    }

    public void setC(int c) {
        this.c = c;
    }
}

class OutCount{
    public Count getCnt() {
        return cnt;
    }

    public void setCnt(Count cnt) {
        this.cnt = cnt;
    }

    Count cnt;


}
