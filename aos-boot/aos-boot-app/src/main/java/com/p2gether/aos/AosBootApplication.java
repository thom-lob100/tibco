package com.p2gether.aos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AosBootApplication {

    public static void main(String[] args) {
        SpringApplication.run(AosBootApplication.class, args);
    }
}
