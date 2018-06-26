package com.util

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.{ObjectListing, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import play.api.libs.json.{JsObject, Json}

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
    val br = new BufferedReader(new InputStreamReader(new GZIPInputStream(s3Object.getObjectContent)))

    // Concatenate multiple lines, if any.
    br.lines.collect(Collectors.joining)

  } finally {
    s3Object.close()
  }


  val messageType = """\{\"messageType\"""".r

  def asJsObjects(s: String): List[JsObject] = {

    // Each sub string should be a valid JSON object
    def subStrings(start: Int, rest: List[Int]): List[String] = rest match {
      case Nil => List(s.substring(start, s.length))
      case x :: xs => s.substring(start, x) :: subStrings(x, xs)
    }

    // Transform each sub string into a JsObject
    def jsObjects(input: List[String]): List[JsObject] = input match {
      case Nil => Nil
      case x :: xs => try {
        Json.parse(x).as[JsObject] :: jsObjects(xs)
      } catch {
        case t: Throwable =>
          println(t)
          jsObjects(xs)
      }
    }

    val matches = messageType.findAllMatchIn(s).toList

    if (matches.isEmpty) Nil else {
      // Get the starting index of each Match
      val head :: tail = matches.map(_.start)
      // Extract the sub-strings
      val strings = subStrings(head, tail)
      // Now try to parse each string into a JsObject
      jsObjects(strings)
    }
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
      val s = getContentAsString(s3Object)
      val ss = asJsObjects(s)
      println(ss)
      i += ss.length
      println(i)
    })
  }
}
