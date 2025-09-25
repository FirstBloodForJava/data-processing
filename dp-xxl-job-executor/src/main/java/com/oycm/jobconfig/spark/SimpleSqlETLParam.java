package com.oycm.jobconfig.spark;

import com.oycm.enums.SparkJobType;
import com.oycm.spark.jdbc.DataEtlConfig;
import com.oycm.spark.jdbc.ReadJdbcConfig;
import com.oycm.spark.jdbc.WriteJdbcConfig;
import lombok.Data;

import java.util.List;

/**
 * 简单 sql etl 配置类
 * @author ouyangcm
 * create 2025/9/19 16:21
 */
@Data
public class SimpleSqlETLParam {

    private long jobId;

    private String jobType;

    private SparkJobType sparkJobType;

    /**
     * jdbc 读配置
     */
    private List<ReadJdbcConfig> readJdbcConfigs;

    /**
     * 读数据转换配置
     */
    private DataEtlConfig dataEtlConfig;

    /**
     * jdbc 写配置
     */
    private WriteJdbcConfig writeJdbcConfig;


}
