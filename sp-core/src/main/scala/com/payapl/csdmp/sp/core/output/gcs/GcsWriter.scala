package com.payapl.csdmp.sp.core.output.gcs

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, BlobInfo, Storage, StorageOptions}
import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import org.apache.spark.sql.DataFrame

import java.util

/** Config to write data to Gcs
 *
 * - step:
 * name: gcsWriteStep
 * outputType:
 * gcsOutput:
 * gcsOutputPath:
 * dataFrameName:
 *
 * @param name not used
 * @param dataFrameName
 * @param projectId
 * @param gcsOutputPath
 * @param gcsOutputName
 * @param outputMode
 * @param format
 * @param isCompose
 * @param options
 */
case class GcsWriter(name: String,
                     dataFrameName: String,
                     projectId: Option[String],
                     gcsOutputPath: String,
                     gcsOutputName: Option[String],
                     outputMode: String = "overwrite",
                     format: String,
                     isCompose: Option[Boolean],
                     options: Option[Map[String, String]]
                    ) extends Writer {

  //compose addSource method can put as many objects here as you want,up to the max of 32
  private val MAX_COMPOSE_FILES_NUMBER = 32

  override def write(dataFrame: DataFrame): Unit = {
    logInfo("bucket path is: " + gcsOutputPath)

    dataFrame
      .coalesce(MAX_COMPOSE_FILES_NUMBER)
      .write
      .mode(outputMode)
      .options(options.getOrElse(Map()))
      .format(format)
      .save(gcsOutputPath)

    if (isCompose.getOrElse(false)) {
      if (gcsOutputName.isEmpty) {
        throw new InvalidArgumentException("gcsOutputName must be defined if isCompose is true")
      }
      val bucket: String = getBucketName(gcsOutputPath)
      val storage: Storage = StorageOptions.newBuilder.setProjectId(projectId.get).build().getService

      val objectPath: String = getBucketName(gcsOutputPath)
      val objectPrefix: String = objectPath + "/" + "part-"
      val targetObject: String = objectPath + "/" + gcsOutputName.get

      // get all blob under this folder
      val blobs: Page[Blob] = storage.list(
        bucket,
        Storage.BlobListOption.prefix(objectPrefix)
      )

      var objectList: List[String] = List[String]()
      val blobIterator: util.Iterator[Blob] = blobs.iterateAll.iterator
      while (blobIterator.hasNext) {
        val blob: Blob = blobIterator.next()
        objectList = blob.getName +: objectList
      }

      //do compose request
      val composeRequest: Storage.ComposeRequest = Storage.ComposeRequest.newBuilder()
        .setTarget(BlobInfo.newBuilder(bucket, targetObject).build())
        .addSource(objectList: _*)
        .build()

      val compositeObject: Blob = storage.compose(composeRequest)
      logInfo("New Composite object " + compositeObject.getName)

      //delete old blobs
      val deleteBlobIterator: util.Iterator[Blob] = blobs.iterateAll.iterator
      while (deleteBlobIterator.hasNext) {
        val blob: Blob = deleteBlobIterator.next()
        blob.delete()
      }
      logInfo("Deleted old blobs object!")
    }


  }

  private def getBucketName(path: String): String = {
    val bucketAndObject: String = path.split("//")(1)
    val bucketAndObjectArray: Array[String] = bucketAndObject.split("/")
    bucketAndObjectArray(0)
  }

  private def getObject(path: String): String = {
    val bucketAndObject: String = path.split("//")(1)
    val bucketAndObjectArray: Array[String] = bucketAndObject.split("/")
    if (bucketAndObjectArray.length == 1) {
      throw new InvalidArgumentException("There is no object included")
    } else {
      val lenOfBucketName: Int = bucketAndObjectArray(0).length
      bucketAndObject.substring(lenOfBucketName + 1)
    }
  }
}
