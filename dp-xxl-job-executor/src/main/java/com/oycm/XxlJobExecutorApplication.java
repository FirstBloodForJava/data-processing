package com.oycm;

import com.oycm.autoconfigure.SparkJobProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author ouyangcm
 * create 2025/9/19 15:52
 */
@SpringBootApplication
@EnableConfigurationProperties(SparkJobProperties.class)
public class XxlJobExecutorApplication {

    public static void main(String[] args) {
        SpringApplication.run(XxlJobExecutorApplication.class, args);
        while (true) {

        }
    }

}
