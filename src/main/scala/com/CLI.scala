package com

import java.io.File

import com.util.Filtering._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalaz.{Failure, Success, ValidationNel, \/}

/*
  Command Line Interface for processing archived logs. Requires the following inputs:
    1 - Starting date/time (inclusive)
    2 - Ending date/time (inclusive)
    3 - Environment
    4 - Log type.
    5 - Destination file name.

  Archived logs are merged and aggregated by environment. Entries are also compressed.
  Therefore a utility is needed to make retrieval manageable.
 */
object CLI {

  val dateTimePattern = "YYYY/MM/dd/HH"
  val formatter = DateTimeFormat.forPattern(dateTimePattern).withZoneUTC()

  def validateDateTime(str: String): ValidationNel[String, DateTime] = \/.fromTryCatchNonFatal(
    DateTime.parse(str, formatter)
  ).leftMap(_ => s"Invalid date/time format: $str, valid format is $dateTimePattern ").validationNel

  def validateEnvironment(str: String): ValidationNel[String, String] =
    (if (environments.contains(str)) Success(str) else
      Failure(s"Invalid environment: $str, should be one of ${environments.mkString(", ")}")).toValidationNel

  def validateLogType(str: String): ValidationNel[String, String] =
    (if (logTypes.contains(str)) Success(str) else
      Failure(s"Invalid log type: $str, should be one of ${logTypes.mkString(", ")}")).toValidationNel

  def validateDestinationFile(str: String): ValidationNel[String, File] = \/.fromTryCatchNonFatal {
    val file = new File(str)
    file.createNewFile()
    file
  }.leftMap(_ => s"Invalid file path: $str").validationNel
}
