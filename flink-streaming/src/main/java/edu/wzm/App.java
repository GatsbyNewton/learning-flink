package edu.wzm;

import java.net.URL;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args)throws Exception {
        URL fileUrl = App.class.getClassLoader().getResource("UserBehavior.csv");
        System.out.println(fileUrl.toURI().getPath());
    }
}
