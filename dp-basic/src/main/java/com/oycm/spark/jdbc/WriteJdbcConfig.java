package com.oycm.spark.jdbc;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Spark Jdbc 写配置
 * @author ouyangcm
 * create 2025/9/23 11:33
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class WriteJdbcConfig extends JdbcConfig {

    /**
     * JDBC 批处理大小
     */
    private int batchSize = 1000;

    /**
     * 设置当前连接的事务隔离级别, 支持: NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE
     */
    private String isolationLevel = "READ_UNCOMMITTED";

    /**
     * truncate = true, 写入模式为 SaveMode.Overwrite, 写入的表存在, Spark 通过 truncate table, 而不是删除再创建。这样不会丢失表的元数据(例如: 索引)
     * 对于不支持 truncate 的数据库，忽略此参数。
     * MySQLDialect、DB2Dialect、MsSqlServerDialect、DerbyDialect 和 OracleDialect
     */
    private boolean truncate = false;

    /**
     * 是否安全执行 truncate
     */
    private boolean cascadeTruncate = false;

    /**
     * 创建表是附带的参数, 例如: ENGINE=InnoDB DEFAULT CHARSET=utf8
     * CREATE TABLE t (name string) ENGINE=InnoDB
     */
    private String createTableOptions;

    /**
     * 创建表时, 列使用的类型, 而不是推断的类型, 指定的类型需要符合 Spark SQL 数据类型
     */
    private String createTableColumnTypes;


    // 自定义参数
    /**
     * 自定义 CusSaveMode.Update 数据唯一标志
     */

    private String updateUniqueKey;

    /**
     * 写入模式
     */
    private String saveMode = "Append";

}
