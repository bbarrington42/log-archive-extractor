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

    // Concatenate multiple lines, if any.
    br.lines().reduce("", op)

  } finally {
    s3Object.close()
  }

  def extract(s: String, start: Int = 0): List[String] = {
    println("Entering extract...")
    println(s"start: $start")

    val chars = s.toList

    // Return the index of the character AFTER the matching closing brace
    def findClosingBrace(index: Int, count: Int = 0): Int = {
      println(s"Entering findClosingBrace..., index=$index, count=$count")
      if (index >= chars.length) chars.length else chars(index) match {
        case '{' => findClosingBrace(index + 1, count + 1)
        case '}' => if (count == 1) index + 1 else findClosingBrace(index + 1, count - 1)
        case _ => findClosingBrace(index + 1, count)
      }
    }

    val rv = if (start >= s.length) Nil else {
      val end = findClosingBrace(start)
      println(s"start: $start, end: $end")
      s.substring(start, end) :: extract(s, end)
    }

    println("Exiting extract...")

    rv
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

    var i = 0

    iter.foreach(s3Object => {
      // todo Consider not retrieving S3Object. Instead just use the getContentAsString method on the S3 client.
      val s = getContentAsString(s3Object)
      val ss = extract(s)
      println(ss)
      i += ss.length
      println(i)
    })
  }
}
