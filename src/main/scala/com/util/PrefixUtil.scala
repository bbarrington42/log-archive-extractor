package com.util

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

import scala.collection.JavaConverters._

/*
  Utility to obtain all common prefixes within a range.
 */


object PrefixUtil {

  // Get all common prefixes for a given bucket and key.
  def getPrefixes(s3: AmazonS3, bucket: String, key: String): List[String] = {
    def prefixes(objectListing: ObjectListing): List[String] =
      objectListing.getCommonPrefixes.asScala.toList ++
        (if (objectListing.isTruncated) prefixes(s3.listNextBatchOfObjects(objectListing)) else Nil)

    val request = new ListObjectsRequest().withBucketName(bucket).withPrefix(key).withDelimiter("/")

    val rv = prefixes(s3.listObjects(request))

    println(s"getPrefixes: $rv for prefix $key")

    rv
  }


  // Recurse down the hierarchy of prefixes.
  def allPrefixes(s3: AmazonS3, bucketName: String, prefix: String): List[String] = {

    def fanOut(prefixes: List[String]): List[String] = prefixes match {
      case Nil => Nil
      case x :: xs => fanOut(getPrefixes(s3, bucketName,x)) ++ fanOut(xs)
    }

    fanOut(getPrefixes(s3, bucketName, prefix))
  }
}


object TestPrefixUtil {
  import PrefixUtil._

  def main(args: Array[String]): Unit = {

    val credentialsProvider = new ProfileCredentialsProvider("cda")

    val s3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider)

    val s3 = s3ClientBuilder.build()

    val result = allPrefixes(s3, "cda_logs", "prod-green/")

    println(result)
  }
}
