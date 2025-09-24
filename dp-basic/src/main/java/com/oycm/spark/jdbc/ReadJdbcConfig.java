package com.oycm.spark.jdbc;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Spark Jdbc 读配置
 * @author ouyangcm
 * create 2025/9/23 11:33
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class ReadJdbcConfig extends JdbcConfig {

    /**
     * 数据分区的字段名, 必须 numeric, date, or timestamp 类型
     */
    private String partitionColumn;

    /**
     * partitionColumn 边界
     */
    private String lowerBound;

    /**
     * partitionColumn 边界
     */
    private String upperBound;


    /**
     * 每次执行放回的数据量
     */
    private int fetchSize = 1000;

    /**
     * 和远程数据库建立会话后, 在开始读取数据前, 执行的自定义 PL/SQL 块
     */
    private String sessionInitStatement;

    /**
     * Spark 会根据表的 metadata 数据推断字段类型
     * 显式指定字段映射 Spark SQL 数据类型
     */
    private String customSchema;

    /**
     * 是否将 where 条件推送到 JDBC 数据源
     */
    private boolean pushDownPredicate = true;

    /**
     * 在 V2 JDBC 数据源, 是否推送 aggregate
     */
    private boolean pushDownAggregate = true;

    /**
     * 在 V2 JDBC 数据源, 是否添加 limit
     */
    private boolean pushDownLimit = true;

    /**
     *
     */
    private boolean pushDownOffset = true;

    /**
     *
     */
    private boolean pushDownTableSample = true;

    /**
     * true: 没有时区的 TIMESTAMP 转换成 TimestampNTZ
     * false:
     */
    private boolean preferTimestampNTZ = false;


    // 自定义相关参数
    /**
     * 创建的临时视图名
     */
    private String name;

    /**
     * 是否 cache
     */
    private boolean cache = false;

}
