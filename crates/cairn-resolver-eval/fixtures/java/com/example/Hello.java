package com.example;

public class Hello implements Greeter {
    @Override
    public String greet(String name) {
        return "hello, " + name;
    }
}
