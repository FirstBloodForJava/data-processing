package com.oycm.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author ouyangcm
 * create 2025/9/19 16:03
 */
@ConfigurationProperties(prefix = "dp.config")
@Data
public class SparkJobProperties {

    /**
     * bin/spark-submit --master 配置
     */
    private String master;

    /**
     * bin/spark-submit 上下文路径配置
     */
    private String relativePath;

    /**
     * bin/spark-submit 提交默认参数
     */
    private String defaultJobConf;
}
