package com.util

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ObjectListing, S3Object}

import scala.collection.JavaConverters._

/*
  An iterator for a collection of objects in S3. This handles truncated lists.
 */
class S3ObjectIterator(s3: AmazonS3, objectListing: ObjectListing) extends Iterator[S3Object] {

  private var pageIterator = new PageIterator(objectListing)

  override def hasNext: Boolean = pageIterator.hasNext || {
    if (pageIterator.objectListing.isTruncated)
      pageIterator = new PageIterator(s3.listNextBatchOfObjects(pageIterator.objectListing))
    pageIterator.hasNext
  }

  override def next(): S3Object = pageIterator.next()

  class PageIterator(val objectListing: ObjectListing) extends Iterator[S3Object] {

    private var index = 0

    private val summaries = objectListing.getObjectSummaries.asScala
    private val bucketName = objectListing.getBucketName


    override def hasNext: Boolean = index < objectListing.getObjectSummaries.size

    override def next(): S3Object = {
      val i = index
      index += 1
      s3.getObject(bucketName, summaries(i).getKey)
    }

  }

}

object Main {
  def main(args: Array[String]): Unit = {

    val credentialsProvider = new ProfileCredentialsProvider("cda")

    val s3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider)

    val s3 = s3ClientBuilder.build()

    // For now, just retrieve all logs in the 'test' environment
    val objectListing = s3.listObjects("cda_logs", "test")

    val iter = new S3ObjectIterator(s3, objectListing)

    while(iter.hasNext) {
      val next = iter.next()
      println(next)
    }
  }
}
