package com.oycm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.TimeUnit;

/**
 * @author ouyangcm
 * create 2025/9/19 15:52
 */
@SpringBootApplication
public class XxlJobExecutorApplication {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(XxlJobExecutorApplication.class, args);
        while (true) {
            TimeUnit.MINUTES.sleep(60);
        }
    }

}
