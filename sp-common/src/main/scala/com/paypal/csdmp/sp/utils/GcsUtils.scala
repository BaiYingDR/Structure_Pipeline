package com.paypal.csdmp.sp.utils

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import org.apache.commons.lang.StringUtils
import java.io.{InputStream}
import java.net.URI
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object GcsUtils {

  private var storage: Storage = StorageOptions.getDefaultInstance.getService

  def setStorage(storage: Storage) = {
    this.storage = storage
  }

  def cleanDir(path: String) = {
    val (bucketName, objectName) = getBucketNameAndObjectNameFromPath(path)
    cleanDir(bucketName, objectName)
  }

  def cleanDir(bucketName: String, objectName: String) = {
    val dir: String = if (objectName.endsWith("/")) objectName else s"$objectName/"
    val blobs: Page[Blob] = storage.list(bucketName, Storage.BlobListOption.prefix(dir))
    blobs.iterateAll().foreach(_.delete())
  }

  def listDir(path: String): List[String] = {
    val (bucketName, objectName) = getBucketNameAndObjectNameFromPath(path)
    listDir(bucketName, objectName)
  }

  def listDir(bucketName: String, objectName: String) = {
    val dir: String = if (objectName.endsWith("/")) objectName else s"$objectName/"
    val blobs: Page[Blob] = storage.list(bucketName, Storage.BlobListOption.prefix(dir))
    blobs.iterateAll().map(_.getBlobId.toGsUtilUri).toList
  }

  def copy(source: String, target: String, delSrc: Boolean = false) = {
    val src = storage.get(buildBlobId(source))
    src.copyTo(buildBlobId(target))
    if (delSrc) src.delete()
  }

  /**
   * read content from file on gcs bucket
   *
   * @param path
   * @return
   */
  def readInputStream(path: String): InputStream = readInputStream(storage.get(buildBlobId(path)))

  /**
   * read optional content from file on gcs bucket
   *
   * @param path
   * @return
   */
  def readOpt(path: String) = {
    val blobId: BlobId = buildBlobId(path)
    Option(storage.get(blobId)).map(read(_))
  }

  /**
   * read content from file on gcs bucket
   *
   * @param source
   * @return
   */
  def read(source: String) = read((storage.get(buildBlobId(source))))

  /**
   * read content from file on gcs bucket
   *
   * @param bucketName
   * @param objectName
   * @return
   */
  def read(bucketName: String, objectName: String): String = read(storage.get(BlobId.of(bucketName, objectName)))

  /**
   *
   * @param bucketName
   * @param objectName
   * @param is
   * @return
   */
  def write(bucketName: String, objectName: String, is: InputStream) = storage.createFrom(buildBlobInfo(bucketName, objectName), is)

  /**
   *
   * @param path
   * @param is
   * @return
   */
  def write(path: String, is: InputStream) = storage.createFrom(buildBlobInfo(path), is)

  /**
   *
   * @param bucketName
   * @param objectName
   * @param content
   * @return
   */
  def write(bucketName: String, objectName: String, content: String) = storage.create(buildBlobInfo(bucketName, objectName), content.getBytes(StandardCharsets.UTF_8))

  /**
   *
   * @param path
   * @param content
   */
  def write(path: String, content: String): Unit = storage.create(buildBlobInfo(path), content.getBytes(StandardCharsets.UTF_8))

  /**
   *
   * @param blob
   * @return
   */
  def readInputStream(blob: Blob): InputStream = Channels.newInputStream(blob.reader())

  /**
   *
   * @param path
   * @return
   */
  def createOutputStream(path: String) = Channels.newOutputStream(storage.writer(buildBlobInfo(path)))

  /**
   *
   * @param blob
   * @return
   */
  def read(blob: Blob): String =
    Using(Channels.newInputStream(blob.reader()))(inputStream => IOUtils.toString(inputStream, StandardCharsets.UTF_8))

  /**
   *
   * @param source
   * @return
   */
  def buildBlobInfo(path: String): BlobInfo = BlobInfo.newBuilder(buildBlobId(path)).build()

  /**
   *
   * @param bucketName
   * @param objectName
   * @return
   */
  private def buildBlobInfo(bucketName: String, objectName: String): BlobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, objectName)).build()

  /**
   *
   * @param source
   * @return
   */
  private def buildBlobId(path: String): BlobId = {
    var (bucketName, objectName) = getBucketNameAndObjectNameFromPath(path)
    BlobId.of(bucketName, objectName)
  }

  private def getBucketNameAndObjectNameFromPath(path: String): (String, String) = {
    val uri = URI.create(path)
    (uri.getHost, StringUtils.stripStart(uri.getPath, "/"))
  }

  def getBucketname(path: String) = {
    URI.create(path).getHost
  }

  def deleteObject(path: String) = {
    val (bucketName, objectName) = getBucketNameAndObjectNameFromPath(path)
    val blob: Blob = storage.get(BlobId.of(bucketName, objectName))
    if (blob != null && blob.exists()) blob.delete()
  }

  def exists(path: String) = {
    val (bucketName, objectName) = getBucketNameAndObjectNameFromPath(path)
    val blob: Blob = storage.get(BlobId.of(bucketName, objectName))
    if (blob != null && blob.exists()) true else false
  }
}
