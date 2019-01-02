package com.datastax.spark.connector.writer

import scala.reflect.runtime.universe._
import org.apache.spark.sql.ForeachWriter
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector

.CollectionColumnName

import scala.collection.{IndexedSeq, Seq}


class CassandraSink[T : TypeTag : ColumnMapper](table: String,
                       keyspace: String,
                       connector: CassandraConnector,
                       columnSelector: ColumnSelector,
                       writeConf: WriteConf) extends ForeachWriter[T] {
  val tableDef = Schema.tableFromCassandra(connector, keyspace, table)
  val selectedColumns = columnSelector.selectFrom(tableDef)
  val optionColumns = writeConf.optionsAsColumns(tableDef.keyspaceName, tableDef.tableName)
  val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
    tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
    selectedColumns ++ optionColumns.map(_.ref))
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)
  private val isCounterUpdate = tableDef.columns.exists(_.isCounterColumn)
  private val containsCollectionBehaviors = selectedColumns.exists(_.isInstanceOf[CollectionColumnName])
  private val isIdempotent = TableWriter.isIdempotent(columns, selectedColumns, tableDef)
  private val template =
    if (isCounterUpdate || containsCollectionBehaviors)
      TableWriter.queryTemplateForInsert(tableDef, columnNames, writeConf)
    else
      TableWriter.queryTemplateForUpdate(tableDef, columns, selectedColumns, writeConf)

  def process(record: T): Unit =
    connector.withSessionDo { session =>
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      val stmt = TableWriter.prepareStatement(template, session, isIdempotent).setConsistencyLevel(writeConf.consistencyLevel)
      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = writeConf.ignoreNulls)
      session.executeAsync(boundStmtBuilder.bind(record))
    }

  def open(partitionId: Long, version: Long): Boolean = true

  def close(errorOrNull: Throwable): Unit = { throw new RuntimeException("cause i want to")}
}