package org.apache.spark.sql.execution.datasources.jdbc


import java.sql.{Connection, PreparedStatement, SQLException}
import java.util
import scala.util.control.{Breaks, NonFatal}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{instantToMicros, localDateToDays, toJavaDate, toJavaTimestamp}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getJdbcType
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._

import java.time.{Instant, LocalDate}
import java.util.Locale


/**
 * Update/Insert 写入模式 jdbc 工具
 */
object CusJdbcUtils extends Logging with SQLConfHelper {

  /**
   * 生成
   */
  def getUpdateStatement(
                          table: String,
                          rddSchema: StructType,
                          tableSchema: Option[StructType],
                          dialect: JdbcDialect,
                          whereColumnKey: String): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name))
    } else {
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName = tableColumnNames.find(f => conf.resolver(f, col.name)).getOrElse {
          throw QueryCompilationErrors.columnNotFoundInSchemaError(col, tableSchema)
        }
        dialect.quoteIdentifier(normalizedName)
      }
    }
    val duplicateSetting = columns.map(x => s"$x=?").mkString(",")
    s"UPDATE $table SET $duplicateSetting WHERE $whereColumnKey=?"
  }

  /**
   * 参考 JdbcUtils.savePartition
   */
  def cusSavePartition(
                        iterator: Iterator[Row],
                        rddSchema: StructType,
                        insertSql: String,
                        updateSql: String,
                        dialect: JdbcDialect,
                        options: JdbcOptionsInWrite,
                        updateUniqueKey: String): Unit = {

    if (iterator.isEmpty) {
      return
    }
    val table = options.table
    // 未配置默认 READ_UNCOMMITTED
    val isolationLevel = options.isolationLevel
    // 先查判断是否存在, 所有限制批写入大小
    val batchSize = if (options.batchSize > 1000) 1000 else options.batchSize

    val outMetrics = TaskContext.get().taskMetrics().outputMetrics

    val conn = dialect.createConnectionFactory(options)(-1)
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          // 是否支持配置的 isolationLevel
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            finalIsolationLevel = isolationLevel
          } else {
            // 配置事务不支持, 打印提示
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          // 不支持事务
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    // 是否支持事务
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }

      val insertStat = conn.prepareStatement(insertSql)
      val updateStat = conn.prepareStatement(updateSql)

      try {
        var i = 0
        // 查找唯一列在 rdd 中的位置
        var fieldIndex: Int = -1
        while (i < rddSchema.fields.length && fieldIndex == -1) {
          if (updateUniqueKey.equals(rddSchema.fields(i).name)) {
            fieldIndex = i
          }
          i += 1
        }
        if (-1 == fieldIndex) {
          throw new SQLException(s"updateUniqueKey not match, please check table: $table, updateUniqueKey is: $updateUniqueKey ")
        }

        var rowCount = 0

        insertStat.setQueryTimeout(options.queryTimeout)
        updateStat.setQueryTimeout(options.queryTimeout)

        val list = new util.ArrayList[Row](batchSize)
        while (iterator.hasNext) {
          list.add(iterator.next())
          rowCount += 1
          totalRowCount += 1
          // 批次执行任务
          if (rowCount % batchSize == 0) {
            insertUpdateLogic(table, fieldIndex, conn, list, rddSchema, insertStat, updateStat, dialect, options)
            insertStat.executeBatch()
            updateStat.executeBatch()
            rowCount = 0
          }
        }
        // 剩余内容
        if (rowCount > 0) {
          insertUpdateLogic(table, fieldIndex, conn, list, rddSchema, insertStat, updateStat, dialect, options)
          insertStat.executeBatch()
          updateStat.executeBatch()
        }
      } finally {
        insertStat.close()
        updateStat.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e

    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        } else {
          outMetrics.setRecordsWritten(totalRowCount)
        }
        conn.close()
      } else {
        outMetrics.setRecordsWritten(totalRowCount)

        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }


  private def insertUpdateLogic(table: String,
                                fieldIndex: Int,
                                conn: Connection,
                                list: util.ArrayList[Row],
                                rddSchema: StructType,
                                insertStat: PreparedStatement,
                                updateStat: PreparedStatement,
                                dialect: JdbcDialect,
                                options: JDBCOptions): Unit = {
    val field: StructField = rddSchema.fields(fieldIndex)
    val updateUniqueKey: String = field.name
    // JDBCValueSetter
    val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
    val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val numFields = rddSchema.fields.length

    val questionMark: String = list.toArray.map(f => "?").mkString(",")
    val sql = s"select $updateUniqueKey AS ID from $table where $updateUniqueKey in ($questionMark)"
    val queryStat = conn.prepareStatement(sql)
    queryStat.setQueryTimeout(options.queryTimeout)

    val mapperList: util.Map[String, Row] = new util.HashMap[String, Row](list.size())
    for (i <- 0 until list.size()) {
      // 查询参数设置
      setters(fieldIndex).apply(queryStat, list.get(i), i, fieldIndex)
      mapperList.put(getPrimaryKeyVal(list.get(i), fieldIndex, field.dataType), list.get(i))
    }
    list.clear()
    val resultSet = queryStat.executeQuery()

    while (resultSet.next()) {
      val primaryVal = resultSet.getString(1)
      if (mapperList.containsKey(primaryVal)) {
        updateOrInsertBatch(mapperList.get(primaryVal), updateStat, numFields, setters, nullTypes, true, fieldIndex)
        mapperList.remove(primaryVal)
      }
    }
    mapperList.values().forEach(value => {
      updateOrInsertBatch(value, insertStat, numFields, setters, nullTypes, false, fieldIndex)
    })
    // 关闭查询连接
    queryStat.close()
    mapperList.clear()
  }


  /**
   * 将唯一标志列的值转换成 String
   *
   * @return
   */
  private def getPrimaryKeyVal(
                                row: Row,
                                index: Int,
                                dataType: DataType): String = dataType match {
    case IntegerType =>
      "" + row.getInt(index)

    case LongType =>
      "" + row.getLong(index)

    case ShortType =>
      "" + row.getInt(index)

    case ByteType =>
      "" + row.getInt(index)

    case StringType =>
      row.getString(index)

    case t: DecimalType =>
      row.getDecimal(index).toPlainString

    case _ =>
      throw new IllegalArgumentException(s"Primary Key DataType Not Support, dataType is: ${dataType.typeName}")
  }


  private def updateOrInsertBatch(row: Row,
                                  stmt: PreparedStatement,
                                  numFields: Int,
                                  setters: Array[JDBCValueSetter],
                                  nullTypes: Array[Int],
                                  isUpdate: Boolean,
                                  updateIndex: Int): Unit = {
    var i = 0
    while (i < numFields) {
      if (row.isNullAt(i)) {
        stmt.setNull(i + 1, nullTypes(i))
      } else {
        setters(i).apply(stmt, row, i, i)
      }
      i = i + 1
    }
    if (isUpdate) {
      setters(updateIndex).apply(stmt, row, i, updateIndex)
    }
    stmt.addBatch()
  }


  // JdbcUtils 最后一个参数表示 Row 位置
  // 给 select 和 update 语句使用
  private type JDBCValueSetter = (PreparedStatement, Row, Int, Int) => Unit

  private def makeSetter(
                          conn: Connection,
                          dialect: JdbcDialect,
                          dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setInt(pos + 1, row.getInt(rowPos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setLong(pos + 1, row.getLong(rowPos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(rowPos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(rowPos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setInt(pos + 1, row.getShort(rowPos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setInt(pos + 1, row.getByte(rowPos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(rowPos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setString(pos + 1, row.getString(rowPos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](rowPos))

    case TimestampType =>
      if (conf.datetimeJava8ApiEnabled) {
        (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
          stmt.setTimestamp(pos + 1, toJavaTimestamp(instantToMicros(row.getAs[Instant](rowPos))))
      } else {
        (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
          stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](rowPos))
      }

    case TimestampNTZType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setTimestamp(pos + 1,
          dialect.convertTimestampNTZToJavaTimestamp(row.getAs[java.time.LocalDateTime](rowPos)))

    case DateType =>
      if (conf.datetimeJava8ApiEnabled) {
        (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
          stmt.setDate(pos + 1, toJavaDate(localDateToDays(row.getAs[LocalDate](rowPos))))
      } else {
        (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
          stmt.setDate(pos + 1, row.getAs[java.sql.Date](rowPos))
      }

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(rowPos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int, rowPos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](rowPos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int, rowPos: Int) =>
        throw QueryExecutionErrors.cannotTranslateNonNullValueForFieldError(pos)
  }
}