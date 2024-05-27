package com.payapl.csdmp.sp.core.output.hive

import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.consts.Constant.DATASOURCE_FORMAT_HIVE
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import com.paypal.csdmp.sp.utils.SparkFunctionUtils.{SORT_FUNCTION_MAP, addSorts, getWindow}
import com.paypal.csdmp.sp.utils.{DataFrameUtils, DateTimeUtils}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{col, from_utc_timestamp, lit}
import org.apache.spark.sql.types.{DataType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

case class HiveMergeWriter(name: String,
                           dataFrameName: String,
                           database: String,
                           tableName: String,
                           pkColumn: Option[String],
                           sortingColumn: Option[Map[String, String]],
                           createTsColumn: Option[String],
                           updateTsColumn: Option[String],
                           partitionColumn: Option[String],
                           updateColumns: Option[String],
                           timeZone: Option[String],
                           saveMode: Option[String],
                           repartition: Option[Boolean]
                          ) extends Writer {


  override def write(dataFrame: DataFrame): Unit = {
    val session: SparkSession = dataFrame.sparkSession
    session.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    session.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    session.sparkContext.hadoopConfiguration.set("parquet.enable.dictionary", "false")
    session.sqlContext.setConf("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val dbTableName: String = dataFrame + "." + tableName
    val sourceDF: DataFrame = dataFrame
    val targetDF: DataFrame = session.table(dbTableName)

    checkDataFrame(sourceDF, targetDF)


    partitionColumn match {
      case Some(partition) =>
        logInfo("There has partition columns, we read matched part of the table for target")
        readTargetDF(sourceDF, targetDF, partition)

      case _ =>
        logInfo("There is no partition columns, we read whole table for target dataFrame")
        targetDF
    }
    logInfo("Target DataFrame's Schema is: ")
    targetDF.printSchema()

    var mergeDF: DataFrame = mergeDataFrame(sourceDF, targetDF, createTsColumn.get, updateColumn.get, pkColumn.get, sortingColumn.get, timeZone)

    if (repartition.isDefined && repartition.get) {
      mergeDF.cache.foreach(_ => ())
      val partitionCount: Int = DataFrameUtils.optimizeHiveFileCount(mergeDF)
      mergeDF = DataFrameUtils.optimizeRepartition(mergeDF, partitionCount, partitionColumn)
    }

    logInfo(s"Writing into Hive table: $dbTableName")
    mergeDF.write.mode(saveMode.get).format(DATASOURCE_FORMAT_HIVE).insertInto(dbTableName)
  }

  def readTargetDF(sourceDF: DataFrame, targetFrame: DataFrame, partitionColumn: String): Unit = {
    val partitionColumnList: Array[String] = partitionColumn.split(",")
    val partitionColumnNames: Array[String] = partitionColumnList.map(elem => elem.mkString)
    val partitionColumnNamesSq = Seq(partitionColumnNames: _*)

    val distinctPartitionDF: Dataset[Row] = sourceDF.select(partitionColumnList.head, partitionColumnList.tail: _*).distinct()

    val joinCondition: String = partitionColumnList.map(x => "df1." + x + "=df2." + x).mkString(" and ")
    val whereCondition: String = partitionColumnList.map(x => "df2." + x + "IS NOT NULL").mkString(" and ")
    logInfo(s"when reading target df,joinCondition is $joinCondition")
    logInfo(s"when reading target df,whereCondition is $whereCondition")

    val df2: Dataset[Row] = distinctPartitionDF.as("df2")

    var targetDF: Dataset[Row] = targetFrame.as("df1").join(df2, partitionColumnNamesSq, "left").where(whereCondition)
    val columnsListFromSourceTable: Array[String] = targetFrame.columns
    targetDF = targetDF.select(columnsListFromSourceTable.head, columnsListFromSourceTable.tail: _*)

    logInfo(s"Count of read of: ${targetDF.count()}")

    targetDF

  }


  def mergeDataFrame(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, createTsColumn: String, updateTsColumn: String, pkColumn: String, sortingColumn: Map[String, String], timeZone: Option[String]): DataFrame = {
    var sourceDF: DataFrame = sourceDataFrame
    val keyColumnList: Array[String] = pkColumn.split(",")
    val keyColumnNames: Array[String] = keyColumnList.map(elem => elem.mkString)
    val keyColumnNamesSq = Seq(keyColumnNames: _*)

    if (!createTsColumn.equals("false")) {
      val selectFieldList: Array[String] = keyColumnList ++ Array(createTsColumn)
      logInfo(s"Adding create_ts, selectFieldList: ${selectFieldList.mkString(",")}")
      val createTsMapDF: Dataset[Row] = targetDataFrame.select(selectFieldList.head, selectFieldList.tail: _*).distinct()
      val joinCondition: String = keyColumnList.map(x => "df1." + x + "=df2." + x).mkString(" and ")
      logInfo(s"when adding create_ts to source df, joinCondition is $joinCondition")
      val df2: Dataset[Row] = createTsMapDF.as("df2")

      timeZone match {
        case Some(timeZone) =>
          logInfo(s"time zone is defined, it is: $timeZone")
          val utcTimeStr: String = DateTimeUtils.getCurrentUTCTimestampStr

          sourceDF.as("df1")
            .join(df2, keyColumnNamesSq, "left")
            .withColumn(createTsColumn, functions.when(functions.expr("df2." + createTsColumn + "is not null"), functions.expr("df2." + createTsColumn)).otherwise(from_utc_timestamp(lit(utcTimeStr).cast(TimestampType), timeZone)))

        case None =>
          sourceDF.as("df1")
            .join(df2, keyColumnNamesSq, "left")
            .withColumn(createTsColumn, functions.when(functions.expr("df2." + createTsColumn + "is not null"), functions.expr("df2." + createTsColumn)).otherwise(functions.current_timestamp()))
      }
    }
    var allSortingColumn: Map[String, String] = sortingColumn

    if (!updateTsColumn.equals("false")) {
      logInfo(s"Adding update_ts: ${updateTsColumn}")
      if (sourceDF.columns.contains(updateTsColumn)) {
        sourceDF.drop(updateTsColumn)
      }

      timeZone match {
        case Some(timezone) =>
          logInfo(s"time zone is defined, it is ${timezone}")
          val utcTimestampStr: String = DateTimeUtils.getCurrentUTCTimestampStr
          sourceDF = sourceDF.withColumn(updateTsColumn, from_utc_timestamp(lit(utcTimestampStr).cast(TimestampType), timezone))
        case None =>
          logInfo("time zone is not defined")
          sourceDF = sourceDF.withColumn(updateTsColumn, functions.current_timestamp())
      }
      allSortingColumn = allSortingColumn + {
        updateTsColumn -> "desc"
      }
    }

    sourceDF = sourceDF.select(targetDataFrame.columns.head, targetDataFrame.columns.tail: _*)


    val sorts = Option(allSortingColumn.map(col => {
      val sortType: String = col._2
      if (SORT_FUNCTION_MAP.contains(sortType)) {
        SORT_FUNCTION_MAP(sortType)(col._1)
      } else {
        throw new InvalidArgumentException(s"Invalid sort Type: $sortType, please enter invalid sort type!")
      }
    }).toList)

    if (updateColumns.isDefined) {
      logInfo(s"Only update there columns: ${updateColumns.get}")
      val joinCondition: String = keyColumnList.map(x => "df1." + x + "=df2." + x).mkString(" and ")


      val updateCols: Array[String] = updateColumns.get.split(",")
      val updateDfSelectCols = sourceDF.columns.map(c => {
        if (updateCols.contains(c)) {
          col("df1." + c.trim)
        } else {
          col("df2." + c.trim)
        }
      })

      val updateDF = sourceDF.as("df1")
        .join(targetDataFrame.as("df2"), functions.expr(joinCondition))
        .select(updateDfSelectCols: _*)

      val whereCondition: String = keyColumnList.map(x => "df2." + x + "is null").mkString(" or ")
      val insertDF = sourceDF.as("df1")
        .join(targetDataFrame.as("df2"), functions.lit(joinCondition), "left")
        .select("df1.*").where(whereCondition)

      sourceDF = updateDF.union(insertDF)
    }
    val twoTableUnion: Dataset[Row] = sourceDF.union(targetDataFrame)
    val pkColumnList: Some[List[String]] = Some(pkColumn.split(",").map(i => i.mkString).toList)
    var window: WindowSpec = getWindow(pkColumnList, None)
    window = addSorts(window, sorts)
    val resDataFrame: DataFrame = twoTableUnion.withColumn("Rank", functions.row_number().over(window))
      .where("Rank==1")
      .drop("Rank")

    resDataFrame

  }

  def checkDataFrame(source: DataFrame, target: DataFrame): Unit = {

    logInfo("Start to compare the schemas between source and target")
    logInfo(s"source schema is ${source.schema}")
    logInfo(s"source schema is ${target.schema}")

    val sourceSchemas: Map[String, DataType] = source.schema.fields.map(f => f.name -> f.dataType).toMap
    val targetSchemas: Map[String, DataType] = target.schema.fields.map(f => f.name -> f.dataType).toMap

    val missCols: Iterable[String] = targetSchemas.filter(f => !sourceSchemas.contains(f._1)).keys

    if (missCols.nonEmpty) {
      logWarning(s"These columns are exists in target table but missing in source table, ${missCols}, " +
        s"they will be added into source if createTs columns are missing")
    }

    targetSchemas.foreach(f => {
      if (sourceSchemas.contains(f._1) && f._2.typeName.equals(sourceSchemas(f._1).typeName)) {
        logWarning(s"Target Type ${f._1} does not match source Type ${sourceSchemas(f._1).typeName} on column ${f._1}")
      }
    })

    logInfo("Finish compare the schemas and job may fail if there are any warning above")
  }
}
