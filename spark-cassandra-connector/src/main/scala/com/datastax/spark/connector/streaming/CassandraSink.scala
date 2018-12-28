/*package com.datastax.spark.connector.writer

import scala.reflect.runtime.universe._
import org.apache.spark.sql.ForeachWriter
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.writer.{RowWriterFactory, WriteConf, TableWriter, BoundStatementBuilder}
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.ColumnSelector


class CassandraSink[T : TypeTag : ColumnMapper](table: String,
                       keyspace: String,
                       connector: CassandraConnector,
                       columnSelector: ColumnSelector,
                       writeConf: WriteConf) extends ForeachWriter[T] {
  val tableDef = Schema.tableFromCassandra(connector, table, keyspace)
  val selectedColumns = columnNames.selectFrom(tableDef)
  val optionColumns = writeConf.optionsAsColumns(tableDef.keyspaceName, tableDef.tableName)
  val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
    tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
    selectedColumns ++ optionColumns.map(_.ref))
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)

  val isIdempotent = TableWriter.isIdempotent(columns, columnSelector, tableDef)

  // maybe this should happen once per batch (on open)


  def writeOne(queryTemplate: String, data: T) {
    connector.withSessionDo { session =>
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      val stmt = TableWriter.prepareStatement(queryTemplate, session, tableDef).setConsistencyLevel(writeConf.consistencyLevel)
      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = writeConf.ignoreNulls)
      session.executeAsync(boundStmtBuilder.bind(data))
    }
  }
  def open(partitionId: Long, version: Long): Boolean = true

  def process(record: T) = {}

  def close(errorOrNull: Throwable): Unit = {}
}*/