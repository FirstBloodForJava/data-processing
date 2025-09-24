package com.oycm.spark.jdbc;

import lombok.Data;

/**
 * 将 Jdbc read 中间结果转换成 write 配置
 * @author ouyangcm
 * create 2025/9/24 13:09
 */
@Data
public class DataEtlConfig {

    private String sql;

    /**
     * 是否增加固定修改时间列 LAST_UPDATE
     */
    private boolean isUseUpdateCol = false;

    /**
     * 是否将 逻辑&物理 计划打印
     */
    private boolean explain = true;
}
