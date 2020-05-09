package com.xtz.realtime;

/**
 * @ClassName ExceptionDemo
 * @Description TODO
 * @Author xutengzhong
 * @Date 2020/2/24 23:05
 **/
public class ExceptionDemo {

    public static double fun1(int a, int b){

        if(b==0){
            throw new RuntimeException("不能为0");
        }
        return a / b;
    }

    public static void main(String[] args) {
        fun1(1,0);
    }
}
