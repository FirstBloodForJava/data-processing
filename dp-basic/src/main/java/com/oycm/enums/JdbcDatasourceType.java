package com.oycm.enums;

public enum JdbcDatasourceType {

    mysql("MySQL", "com.mysql.cj.jdbc.Driver"),

    oracle("Oracle", "oracle.jdbc.OracleDriver"),

    dm("dmdb", "dm.jdbc.driver.DmDriver"),

    postgresql("PostgreSQL", "org.postgresql.Driver"),

    sqlserver("SQLServer", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    ;

    /**
     * jar 所在的文件夹名
     */
    private String code;

    /**
     * jdbc 驱动全路径名
     */
    private String driver;

    JdbcDatasourceType(String code, String driver) {
        this.code = code;
        this.driver = driver;
    }

    public String getCode() {
        return code;
    }


    public String getDriver() {
        return driver;
    }


    public static JdbcDatasourceType getByJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            throw new NullPointerException("jdbcUrl is null");
        }

        for (JdbcDatasourceType value : JdbcDatasourceType.values()) {
            if (jdbcUrl.contains(":" + value.name() + ":")) {
                return value;
            }
        }
        //throw new IllegalArgumentException("JdbcDatasourceType not support jdbcUrl: " + jdbcUrl);
        return null;
    }
}
