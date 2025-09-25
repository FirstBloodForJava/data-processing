package com.oycm.enums;

/**
 * Spark 任务枚举类
 */
public enum SparkJobType {

    SimpleSqlETL("SimpleSqlETL", "libs/sparkProgram/dp-spark.jar", "com.oycm.etl.v1.SimpleSqlEtlJob"),

    ;

    /**
     * 配置的 spark 任务类型
     */
    private String type;

    /**
     * 该任务类型所在的 jar 位置
     */
    private String jarPath;

    /**
     * 该任务类型对应的启动类
     */
    private String clazz;

    SparkJobType (String type, String jarPath, String clazz) {
        this.type = type;
        this.jarPath = jarPath;
        this.clazz = clazz;
    }

    public String getType() {
        return type;
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getClazz() {
        return clazz;
    }

    public static SparkJobType getByName(String name) {
        for (SparkJobType value : SparkJobType.values()) {
            if (value.name().equals(name) || value.type.equals(name)) {
                return value;
            }
        }

        return null;
    }
}
