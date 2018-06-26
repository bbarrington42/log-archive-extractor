package com

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.util.Filtering._
import com.util.S3ObjectIterator
import com.util.S3ObjectIterator.{asJsObjects, getContentAsString}


object Main {
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
      val jsObjects = asJsObjects(s).filter(jsObject => isDataMessage(jsObject) && isLogType(jsObject, ConsumerLog))

      println(jsObjects)
      i += jsObjects.length
      println(i)
    })
  }
}
