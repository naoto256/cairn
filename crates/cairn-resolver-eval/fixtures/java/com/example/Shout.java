package com.example;

public class Shout implements Greeter {
    @Override
    public String greet(String name) {
        return "HELLO, " + name.toUpperCase() + "!";
    }
}
