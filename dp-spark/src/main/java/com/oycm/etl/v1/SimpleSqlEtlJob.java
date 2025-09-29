package com.oycm.etl.v1;

import com.alibaba.fastjson.JSON;
import com.oycm.spark.jdbc.CusOption;
import com.oycm.spark.jdbc.DataEtlConfig;
import com.oycm.spark.jdbc.ReadJdbcConfig;
import com.oycm.spark.jdbc.WriteJdbcConfig;
import com.oycm.util.FileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.CusSaveMode;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.current_timestamp;

/**
 * 根据传递的 job 参数简单执行的 ETL(Extract Transform Load)
 * @author ouyangcm
 * create 2025/9/23 11:28
 */
public class SimpleSqlEtlJob {

    // 启动参数两个 jobId, jobFilePath
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("SimpleSqlEtlJob need 2 param, first is jobId, second is SimpleSparkJobParam json file path");
            System.exit(0);
        }

        String jobId = args[0];
        String jobFile = args[1];
        System.out.println("jobId is: " + jobId + ", jobFile is: " + jobFile);

        SimpleSparkJobParam jobParam = JSON.parseObject(FileUtil.getFileContent(jobFile), SimpleSparkJobParam.class);
        // 任务参数校验
        String checkResult = jobParam.check();
        if (checkResult.length() > 0) {
            System.out.println("SimpleSparkJobParam verification failed: \n" + checkResult);
            return;
        }

        SparkSession spark = SparkSession.builder()
                .appName("SimpleSqlEtlJob-" + jobId)
                .config("spark.sql.codegen.wholeStage", true)
                //.master("local[2]")
                .getOrCreate();

        AtomicLong writeCount = new AtomicLong(0L);
        spark.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
                if (taskEnd.taskMetrics() != null && null != taskEnd.taskMetrics().outputMetrics()) {
                    writeCount.getAndAdd(taskEnd.taskMetrics().outputMetrics().recordsWritten());
                }
            }
        });

        spark.log().info("job [{}] begin exec", jobId);

        boolean isSuccess = false;
        try {
            sparkRun(spark, jobParam);
            isSuccess = true;
        } finally {
            spark.stop();
            spark.log().info("[recordsWrittenCount]: {}", writeCount.get());
            spark.log().info("[runStatus]:{}", isSuccess);
        }
    }

    public static void sparkRun(SparkSession spark, SimpleSparkJobParam jobParam) {

        List<Dataset<Row>> datasetRList = jobParam.getReadJdbcConfigs().stream().map(reader -> jdbcRead(spark, reader)).collect(Collectors.toList());

        // 数据转换为 null 取第一个 read Dataset
        if (jobParam.getDataEtlConfig() == null) {
            jdbcWrite(datasetRList.get(0), jobParam.getWriteJdbcConfig());
        }else {
            DataEtlConfig etlConfig = jobParam.getDataEtlConfig();
            Dataset<Row> eltData = spark.sql(etlConfig.getSql());
            if (etlConfig.isUseUpdateCol()) {
                eltData = eltData.withColumn("LAST_UPDATE", current_timestamp());
            }
            if (etlConfig.isExplain()) {
                eltData.explain(true);
            }

            jdbcWrite(eltData, jobParam.getWriteJdbcConfig());
        }

    }


    public static Dataset<Row> jdbcRead(SparkSession spark, ReadJdbcConfig reader) {
        DataFrameReader dfr = spark.read().format("jdbc")
                .option("user", reader.getUser())
                .option("password", reader.getPassword())
                .option(JDBCOptions.JDBC_URL(), reader.getUrl())
                .option(JDBCOptions.JDBC_DRIVER_CLASS(), reader.getDriver());

        if (reader.getNumPartitions() > 0) {
            dfr.option(JDBCOptions.JDBC_NUM_PARTITIONS(), reader.getNumPartitions());
        }

        if (reader.getQueryTimeout() > 0) {
            dfr.option(JDBCOptions.JDBC_QUERY_TIMEOUT(), reader.getQueryTimeout());
        }

        if (StringUtils.isNotBlank(reader.getPrepareQuery())) {
            dfr.option(JDBCOptions.JDBC_PREPARE_QUERY(), reader.getPrepareQuery());
        }

        if (StringUtils.isNotBlank(reader.getConnectionProvider())) {
            dfr.option(JDBCOptions.JDBC_CONNECTION_PROVIDER(), reader.getConnectionProvider());
        }


        // 单独读配置
        if (StringUtils.isNotBlank(reader.getQuery())) {
            dfr.option(JDBCOptions.JDBC_QUERY_STRING(), reader.getQuery());
        } else if (StringUtils.isNotEmpty(reader.getDbTable())) {
            dfr.option(JDBCOptions.JDBC_TABLE_NAME(), reader.getDbTable());
        }

        if (StringUtils.isNotBlank(reader.getPartitionColumn())) {
            dfr.option(JDBCOptions.JDBC_PARTITION_COLUMN(), reader.getPartitionColumn());
            dfr.option(JDBCOptions.JDBC_LOWER_BOUND(), reader.getLowerBound());
            dfr.option(JDBCOptions.JDBC_UPPER_BOUND(), reader.getUpperBound());
        }

        if (reader.getFetchSize() > 0) {
            dfr.option(JDBCOptions.JDBC_BATCH_FETCH_SIZE(), reader.getFetchSize());
        }

        if (StringUtils.isNotBlank(reader.getCustomSchema())) {
            dfr.option(JDBCOptions.JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES(), reader.getCustomSchema());
        }

        if (StringUtils.isNotBlank(reader.getSessionInitStatement())) {
            dfr.option(JDBCOptions.JDBC_SESSION_INIT_STATEMENT(), reader.getSessionInitStatement());
        }

        dfr.option(JDBCOptions.JDBC_PUSHDOWN_PREDICATE(), reader.isPushDownPredicate())
                .option(JDBCOptions.JDBC_PUSHDOWN_AGGREGATE(), reader.isPushDownAggregate())
                .option(JDBCOptions.JDBC_PUSHDOWN_LIMIT(), reader.isPushDownLimit())
                .option(JDBCOptions.JDBC_PUSHDOWN_OFFSET(), reader.isPushDownOffset())
                .option(JDBCOptions.JDBC_PUSHDOWN_TABLESAMPLE(), reader.isPushDownTableSample())
                .option(JDBCOptions.JDBC_PREFER_TIMESTAMP_NTZ(), reader.isPreferTimestampNTZ());

        Dataset<Row> dsRead = dfr.load();

        if (reader.isCache()) {
            dsRead.cache();
        }

        if (StringUtils.isNotEmpty(reader.getName())) {
            dsRead.createOrReplaceTempView(reader.getName());
        }

        return dsRead;
    }


    public static void jdbcWrite(Dataset<Row> dataset, WriteJdbcConfig writer) {
        String format = writer.isUseCusJdbc() ? "cusJdbc" : "jdbc";
        DataFrameWriter<Row> dfw = dataset.write().format(format)
                .option("user", writer.getUser())
                .option("password", writer.getPassword())
                .option(JDBCOptions.JDBC_URL(), writer.getUrl())
                .option(JDBCOptions.JDBC_DRIVER_CLASS(), writer.getDriver())
                .mode(SaveMode.Append);

        // 公共配置
        if (writer.getNumPartitions() > 0) {
            dfw.option(JDBCOptions.JDBC_NUM_PARTITIONS(), writer.getNumPartitions());
        }
        if (writer.getQueryTimeout() > 0) {
            dfw.option(JDBCOptions.JDBC_QUERY_TIMEOUT(), writer.getQueryTimeout());
        }

        if (StringUtils.isNotBlank(writer.getPrepareQuery())) {
            dfw.option(JDBCOptions.JDBC_PREPARE_QUERY(), writer.getPrepareQuery());
        }

        if (StringUtils.isNotBlank(writer.getConnectionProvider())) {
            dfw.option(JDBCOptions.JDBC_CONNECTION_PROVIDER(), writer.getConnectionProvider());
        }

        // 写单独配置
        // 指定 mode, 自定义情况下无效
        dfw.option(CusOption.CUS_MODE, writer.getSaveMode());
        dfw.option(JDBCOptions.JDBC_TABLE_NAME(), writer.getDbTable());

        if (CusSaveMode.Update.name().equalsIgnoreCase(writer.getSaveMode())) {
            dfw.option(CusOption.UPDATE_UNIQUE_KEY, writer.getUpdateUniqueKey());
        }

        if (writer.getBatchSize() > 0) {
            dfw.option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), writer.getBatchSize());
        }

        if (StringUtils.isNotBlank(writer.getIsolationLevel())) {
            dfw.option(JDBCOptions.JDBC_TXN_ISOLATION_LEVEL(), writer.getIsolationLevel());
        }

        if (StringUtils.isNotBlank(writer.getCreateTableOptions())) {
            dfw.option(JDBCOptions.JDBC_CREATE_TABLE_OPTIONS(), writer.getCreateTableOptions());
        }

        if (StringUtils.isNotBlank(writer.getCreateTableColumnTypes())) {
            dfw.option(JDBCOptions.JDBC_CREATE_TABLE_COLUMN_TYPES(), writer.getCreateTableColumnTypes());
        }
        dfw.option(JDBCOptions.JDBC_TRUNCATE(), writer.isTruncate());
        dfw.option(JDBCOptions.JDBC_CASCADE_TRUNCATE(), writer.isCascadeTruncate());

        dfw.save();
    }
}
