package com.ppdai.realtime;

/**
 * @ClassName NumberUtils
 * @Description TODO
 * @Author xutengzhong
 * @Date 2020/2/26 1:31
 **/
public class NumberUtils {


    public static int getPositiveNum(int num) {

        return num & 0x7fffffff;
    }


    public static void main(String[] args) {
        System.out.println(getPositiveNum(-121));
        System.out.println(getPositiveNum(3));
    }
}
