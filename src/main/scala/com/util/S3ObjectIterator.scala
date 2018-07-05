package com.util

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, S3Object}
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

  private class PageIterator(val objectListing: ObjectListing) extends Iterator[S3Object] {

    private var index = 0

    private val summaries = objectListing.getObjectSummaries.asScala
    private val bucketName = objectListing.getBucketName

    val prefix = objectListing.getPrefix
    val sz = objectListing.getObjectSummaries.size
    println(s"$sz objects found in folder: $prefix")

    override def hasNext: Boolean = index < sz

    override def next(): S3Object = {
      val i = index
      index += 1
      s3.getObject(bucketName, summaries(i).getKey)
    }

  }

}

object S3ObjectIterator {

  def apply(s3: AmazonS3, bucketName: String, prefix: String): S3ObjectIterator =
    new S3ObjectIterator(s3, s3.listObjects(bucketName, prefix))

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
          Json.obj("error" -> t.getMessage) :: jsObjects(xs)
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

