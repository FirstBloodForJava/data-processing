package org.apache.spark.sql.execution.datasources.jdbc;

import org.apache.spark.annotation.Stable;

@Stable
public enum CusSaveMode {

    /**
     * 表存在, 执行 insert
     * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
     * contents of the DataFrame are expected to be appended to existing data.
     *
     * @since 1.3.0
     */
    Append,
    /**
     * 表存在, 则先删表数据/表; 再创建表; 执行 insert
     * Overwrite mode means that when saving a DataFrame to a data source,
     * if data/table already exists, existing data is expected to be overwritten by the contents of
     * the DataFrame.
     *
     * @since 1.3.0
     */
    Overwrite,
    /**
     * 表存在, 抛出异常
     * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
     * an exception is expected to be thrown.
     *
     * @since 1.3.0
     */
    ErrorIfExists,
    /**
     * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
     * the save operation is expected to not save the contents of the DataFrame and to not
     * change the existing data.
     *
     * @since 1.3.0
     */
    Ignore,

    /**
     * Update/Insert 模式: 如果存在该数据，则进行更新，否则则进行插入操作
     */
    Update
    ;

    public static CusSaveMode getByName(String name) {
        for (CusSaveMode saveMode : CusSaveMode.values()) {
            if (saveMode.name().equals(name)) {
                return saveMode;
            }
        }

        return null;
    }
}
