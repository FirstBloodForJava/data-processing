package com.oycm.jobconfig.spark;

import com.oycm.enums.JdbcDatasourceType;
import com.oycm.enums.SparkJobType;
import com.oycm.spark.jdbc.DataEtlConfig;
import com.oycm.spark.jdbc.ReadJdbcConfig;
import com.oycm.spark.jdbc.WriteJdbcConfig;
import lombok.Data;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 简单 sql etl 配置类
 *
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

    private Set<String> datasourceSet = new HashSet<>();

    public String paramCheck() {
        StringBuilder result = new StringBuilder();

        if (readJdbcConfigs == null || readJdbcConfigs.isEmpty()) {
            result.append("readJdbcConfigs is empty; ");
        } else {
            for (int i = 0; i < readJdbcConfigs.size(); i++) {
                ReadJdbcConfig reader = readJdbcConfigs.get(i);
                if (reader.getUrl() == null) {
                    result.append("readJdbcConfigs[").append(i).append("] url is empty; ");
                } else {
                    JdbcDatasourceType datasourceType = JdbcDatasourceType.getByJdbcUrl(reader.getUrl());
                    if (datasourceType == null) {
                        result.append("readJdbcConfigs[").append(i).append("] url not support; ");
                    } else {
                        datasourceSet.add(datasourceType.getCode());
                    }
                }
            }
        }

        if (writeJdbcConfig == null) {
            result.append("writeJdbcConfig is empty; ");
        } else {
            if (writeJdbcConfig.getUrl() == null) {
                result.append("writeJdbcConfig url is empty; ");
            } else {
                JdbcDatasourceType datasourceType = JdbcDatasourceType.getByJdbcUrl(writeJdbcConfig.getUrl());
                if (datasourceType == null) {
                    result.append("writeJdbcConfig url not support; ");
                } else {
                    datasourceSet.add(datasourceType.getCode());
                }
            }

        }

        return result.toString();
    }

}
