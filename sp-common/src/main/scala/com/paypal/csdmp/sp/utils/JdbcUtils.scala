package com.paypal.csdmp.sp.utils

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils =>SparkJdbcUtils}

import java.sql.{Connection, PreparedStatement, ResultSet}
import javax.sql.rowset.CachedRowSet



object JdbcUtils {

  /**
   *
   * @param sql
   * @param handle
   * @param conn
   * @tparam T
   * @return
   */
  def query[T](sql: String)(handle: ResultSet => T)(implicit conn: Connection) = {
    Using.Manager {
      use =>
        val statement: PreparedStatement = use(conn.prepareStatement(sql))
        val rs: ResultSet = use(statement.executeQuery())
        handle(rs)
    }
  }

  /**
   *
   * @param sql
   * @param conn
   * @param m
   * @tparam T
   * @return
   */
  def queryAndCache[T <: CachedRowSet](sql: String)(implicit conn: Connection, m: scala.reflect.Manifest[T]) = {
    query(sql) {
      rs =>
        val rowSet: T = m.runtimeClass.newInstance().asInstanceOf[T]
        rowSet.populate(rs)
        rowSet
    }
  }

  /**
   *
   * @param rs
   * @param handle
   * @tparam T
   * @return
   */
  def handleRowSet[T](rs: CachedRowSet)(handle: CachedRowSet => T): T = handle(rs)

  def buildJdbcConnection(jdbcUrl: String, userName: String, password: String, driver: String, tableName: String = "mock") = {
    val options = new JDBCOptions(jdbcUrl, tableName, Map("user" -> userName, "password" -> password, "driver" -> driver))
    SparkJdbcUtils.createConnectionFactory(options)()
  }
}
