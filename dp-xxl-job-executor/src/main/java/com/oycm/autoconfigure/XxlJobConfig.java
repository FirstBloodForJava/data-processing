package com.oycm.autoconfigure;

import com.oycm.jobhandler.SimpleSparkJobHandler;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * xxj-job 配置类
 * @author ouyangcm
 * create 2025/9/29 10:09
 */
@Configuration
@EnableConfigurationProperties(SparkJobProperties.class)
public class XxlJobConfig {

    private Logger logger = LoggerFactory.getLogger(XxlJobConfig.class);

    @Value("${xxl.job.admin.addresses}")
    private String adminAddresses;

    @Value("${xxl.job.admin.accessToken:default_token}")
    private String accessToken;

    /**
     * 执行器调用调度中心的超时时间, 单位 s
     */
    @Value("${xxl.job.admin.timeout:10}")
    private int timeout;

    @Value("${xxl.job.executor.appName}")
    private String appName;

    @Value("${xxl.job.executor.address:}")
    private String address;

    @Value("${xxl.job.executor.ip}")
    private String ip;

    @Value("${xxl.job.executor.port}")
    private int port;

    @Value("${xxl.job.executor.logPath}")
    private String logPath;

    @Value("${xxl.job.executor.logRetentionDays}")
    private int logRetentionDays;

    @Bean
    public XxlJobSpringExecutor xxlJobExecutor(SparkJobProperties sparkJobProperties) {
        logger.info("xxl-job-executor config init.");
        XxlJobSpringExecutor xxlJobSpringExecutor = new XxlJobSpringExecutor();
        xxlJobSpringExecutor.setAdminAddresses(adminAddresses);
        xxlJobSpringExecutor.setAppname(appName);
        xxlJobSpringExecutor.setAddress(address);
        xxlJobSpringExecutor.setIp(ip);
        xxlJobSpringExecutor.setPort(port);
        xxlJobSpringExecutor.setAccessToken(accessToken);
        xxlJobSpringExecutor.setTimeout(timeout);
        xxlJobSpringExecutor.setLogPath(logPath);
        xxlJobSpringExecutor.setLogRetentionDays(logRetentionDays);

        XxlJobExecutor.registJobHandler("simpleSpark", new SimpleSparkJobHandler(sparkJobProperties));

        return xxlJobSpringExecutor;
    }

}
