package com.oycm.spark.jdbc;

import lombok.Data;

/**
 * Spark Jdbc 读/写 公共配置
 * @author ouyangcm
 * create 2025/9/23 11:32
 */
@Data
public class JdbcConfig {

    /**
     * jdbc 连接地址, oracle: jdbc:oracle:thin:@//ip:1521/service
     *
     */
    private String url;

    /**
     * JDBC 连接的驱动类
     */
    private String driver;

    /**
     * read/write 的表名
     */
    private String dbTable;

    /**
     * dbtable 和 query 不能同时配置
     * read: 查询 sql
     */
    private String query;

    /**
     * read: 对于不支持子查询的数据库, 生成子查询的方式
     * write:
     */
    private String prepareQuery;


    /**
     *  read/write 并行的最大分区数
     */
    private int numPartitions;

    /**
     * Statement 执行等待时间, 0 表示无限制
     */
    private int queryTimeout = 0;


    /**
     * kerberos 认证相关
     */
    private String keytab;

    /**
     *
     */
    private String principal;

    /**
     *
     */
    private boolean refreshKrb5Config;


    /**
     * 使用哪种连接生产者, 不通过默认的 DriverManager 创建连接
     * 默认支持: DB2; MariaDB; MS Sql; Oracle; PostgreSQL
     */
    private String connectionProvider;

    private String user;

    private String password;

    // 自定义配置
    /**
     * 是否使用自定义 jdbc 源
     */
    private boolean UseCusJdbc = false;
}
