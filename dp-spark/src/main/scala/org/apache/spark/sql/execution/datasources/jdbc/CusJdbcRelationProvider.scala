package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import com.oycm.enums.JdbcDatasourceType
import com.oycm.spark.jdbc.CusOption
import org.apache.spark.sql.execution.datasources.jdbc.CusJdbcUtils.cusSavePartition
import org.apache.spark.sql.execution.datasources.jdbc.CusJdbcUtils.getUpdateStatement

class CusJdbcRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def shortName(): String = "cusJdbc"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {
    val options = new JdbcOptionsInWrite(parameters)
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis
    val dialect = JdbcDialects.get(options.url)
    val conn = dialect.createConnectionFactory(options)(-1)
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        var cusMode = CusSaveMode.valueOf(mode.name());
        // 是否传自定义模式
        if (parameters.contains(CusOption.CUS_MODE)) {
          cusMode = CusSaveMode.valueOf(parameters(CusOption.CUS_MODE));
        }
        cusMode match {
          case CusSaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, options.table, df.schema, isCaseSensitive, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options)
            }

          case CusSaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)

          case CusSaveMode.ErrorIfExists =>
            throw QueryCompilationErrors.tableOrViewAlreadyExistsError(options.table)

          case CusSaveMode.Ignore =>
          // 表存在, 则忽略写入操作

          case CusSaveMode.Update =>
            // Update/Insert 模式
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            updateTable(df, tableSchema, isCaseSensitive, options, JdbcDatasourceType.getByJdbcUrl(options.url), parameters)
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }

  private def updateTable(
                           df: DataFrame,
                           tableSchema: Option[StructType],
                           isCaseSensitive: Boolean,
                           options: JdbcOptionsInWrite,
                           jdbcType: JdbcDatasourceType,
                           parameters: Map[String, String]): Unit = {
    val table = options.table
    val dialect = JdbcDialects.get(options.url)
    val rddSchema = df.schema


    // insert sql
    val insertSql = getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect)
    if (!parameters.contains(CusOption.UPDATE_UNIQUE_KEY)
      || null == parameters(CusOption.UPDATE_UNIQUE_KEY)
      || "".equals(parameters(CusOption.UPDATE_UNIQUE_KEY))) {
      throw new AnalysisException(
        s"Update CustomMode Must Have updateUniqueKey!, your updateUniqueKey is:" + parameters.get(CusOption.UPDATE_UNIQUE_KEY))
    }
    // update sql
    val updateSql = getUpdateStatement(table, rddSchema, tableSchema, dialect, parameters(CusOption.UPDATE_UNIQUE_KEY))
    // 打印执行 sql
    println(insertSql, updateSql)

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw QueryExecutionErrors.invalidJdbcNumPartitionsError(
        n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition { iterator =>
      cusSavePartition(
        iterator, rddSchema, insertSql, updateSql, dialect, options, parameters(CusOption.UPDATE_UNIQUE_KEY))
    }
  }

}