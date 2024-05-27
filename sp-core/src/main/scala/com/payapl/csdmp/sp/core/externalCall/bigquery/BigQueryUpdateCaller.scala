package com.payapl.csdmp.sp.core.externalCall.bigquery

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Job, JobId, JobInfo, QueryJobConfiguration}
import com.payapl.csdmp.sp.core.ExternalCall
import org.apache.spark.sql.SparkSession

import java.util.UUID

case class BigQueryUpdateCaller(name: String,
                                projectName: String,
                                datasetName: String,
                                tableName: String,
                                updateFields: String,
                                filter: String,
                                bucketname: String,
                                materializationDataset: String
                               ) extends ExternalCall {

  override def process()(implicit sparkSession: SparkSession): Unit = {

    sparkSession.conf.set("materializationDataset", materializationDataset)
    sparkSession.conf.set("temporaryGcsBucket", bucketname)

    val targetTableName: String = "`" + projectName + "." + datasetName + "." + tableName + "`"
    val updateSql: String = "update " + targetTableName + "SET " + updateFields + " WHERE " + filter
    logInfo("SQL to be executed " + updateSql)

    val bigqueryConn: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(updateSql).setUseLegacySql(false).build()

    val jobId: JobId = JobId.of(UUID.randomUUID().toString)
    var job: Job = bigqueryConn.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

    job = job.waitFor()
    if (job == null) {
      throw new RuntimeException("job no longer exists")
    } else if (job.getStatus.getError != null) {
      throw new RuntimeException(job.getStatus.getError.toString)
    }
  }
}
