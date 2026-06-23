package com.example;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Greeter> greeters = List.of(new Hello(), new Shout());
        for (Greeter g : greeters) {
            System.out.println(g.greet("world"));
        }
    }
}
