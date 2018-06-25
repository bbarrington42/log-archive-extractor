package com.util

import java.io.{BufferedReader, InputStreamReader}
import java.util.function.BinaryOperator
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.{ObjectListing, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.collection.JavaConverters._

/*
  An iterator for a collection of objects in S3. This handles truncated lists.
  Note that an S3Object is returned from the iterator and this object must be closed after use or a resource leak
  will occur.
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

    println(s"Got ${summaries.length} summaries...")

    override def hasNext: Boolean = index < objectListing.getObjectSummaries.size

    override def next(): S3Object = {
      val i = index
      index += 1
      s3.getObject(bucketName, summaries(i).getKey)
    }

  }

}

object S3ObjectIterator {
  // Retrieve object as a String and close the underlying HTTP connection.
  def getContentAsString(s3Object: S3Object): String = try {
    val op = new BinaryOperator[String] {
      override def apply(t: String, u: String): String = t + u
    }
    val br = new BufferedReader(new InputStreamReader(new GZIPInputStream(s3Object.getObjectContent)))
    br.lines().reduce("", op)
  } finally {
    s3Object.close()
  }
}

object Main {
  import S3ObjectIterator._
  
  def main(args: Array[String]): Unit = {

    val credentialsProvider = new ProfileCredentialsProvider("cda")

    val s3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider)

    val s3 = s3ClientBuilder.build()

    // For now, just retrieve all logs in the 'test' environment
    val objectListing = s3.listObjects("cda_logs", "test")

    val iter = new S3ObjectIterator(s3, objectListing)

    iter.foreach(s3Object => println(getContentAsString(s3Object)))
//
//    var i = 1
//    while (iter.hasNext) {
//      val next = iter.next()
//      val r = getContentAsString(next)
//
//      println(r)
//    }
  }
}
