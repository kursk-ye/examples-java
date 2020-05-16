package io.github.streamingwithflink.chapter8.datahub.tuple;

public class TestLoop {
  public static void main(String[] args) {
    //
    System.out.println(run());

  }

  private static int run(){
      Boolean flag = true;
      while(flag){
          System.out.println("loop running");
          return  1;
      }
      return 0;
  }
}
