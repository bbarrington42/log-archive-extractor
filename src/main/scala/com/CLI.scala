package com

import java.io.{BufferedWriter, File, FileWriter, Writer}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.util.Filtering._
import com.util.S3ObjectIterator
import com.util.S3ObjectIterator.{asJsObjects, getContentAsString}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalaz.{Apply, Failure, Success, ValidationNel, \/}

/*
  Command Line Interface for processing archived logs. Requires the following inputs:
    1 - Starting date/time (inclusive)
    2 - Ending date/time (inclusive)
    3 - Environment
    4 - Log type.
    5 - Destination file name.

  Archived logs are merged and aggregated by environment. Entries are also compressed.
  Therefore a utility is needed to make retrieval manageable.

  An example command line invocation:

  extract-logs --env prod-green --log-type consumer --start 2018/06/30/20  --end 2018/07/01/19 --to data/log.txt
 */
object CLI {

  // Build the S3 client
  val credentialsProvider = new ProfileCredentialsProvider("cda")

  val s3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider)

  val s3 = s3ClientBuilder.build()


  val dateTimePattern = "YYYY/MM/dd/HH"
  val formatter = DateTimeFormat.forPattern(dateTimePattern).withZoneUTC()

  private def emptyOrInvalid(str: String, what: String): String =
    if (str.isEmpty) s"Empty $what" else s"Invalid $what"

  // Individual validators...
  def validateDateTime(str: String): ValidationNel[String, DateTime] = \/.fromTryCatchNonFatal(
    DateTime.parse(str, formatter)
  ).leftMap(_ => s"${emptyOrInvalid(str, "date/time")}, format is $dateTimePattern ").validationNel

  def validateEnvironment(str: String): ValidationNel[String, String] =
    (if (environments.contains(str)) Success(str) else
      Failure(s"${emptyOrInvalid(str, "environment")}, should be one of ${environments.mkString(", ")}")).toValidationNel

  def validateLogType(str: String): ValidationNel[String, LogType] =
    (if (logTypes.keySet.contains(str)) Success(logTypes(str)) else
      Failure(s"${emptyOrInvalid(str, "log type")}, should be one of ${logTypes.keySet.mkString(", ")}")).toValidationNel

  def validateDestinationFile(str: String): ValidationNel[String, File] = \/.fromTryCatchNonFatal {
    val file = new File(str)
    file.createNewFile()
    file
  }.leftMap(_ => s"${emptyOrInvalid(str, "file path")}").validationNel


  // Does all the leg work once the parameters have been validated.
  private def extractLogs(s3: AmazonS3, env: String, logType: LogType, start: DateTime, end: DateTime, destination: File): Unit = {

    import com.util.PrefixUtil._

    def fileWriter(file: File)(f: Writer => Unit) = {
      val writer = new BufferedWriter(new FileWriter(file))
      try {
        f(writer)
      } catch {
        case thr: Throwable => println(thr)
      } finally {
        writer.close()
      }
    }

    fileWriter(destination)(
      writer => {
        val ps = prefixes(env, start, end)
        ps.foreach(p => {
          val s3Iter = S3ObjectIterator(s3, "cda_logs", p)
          while (s3Iter.hasNext) {
            val s = getContentAsString(s3Iter.next())
            val jsObjects = asJsObjects(s).filter(jsObject => {
              val p = for {
                b1 <- isDataMessage(jsObject)
                b2 <- matchLogType(jsObject, logType)
              } yield b1 && b2
              p.getOrElse(false)
            })

            writer.write(jsObjects.mkString("\n", "\n", "\n"))
          }
        })
      }
    )
  }

  type VNelT[T] = ValidationNel[String, T]

  // Extract log content if parameters validate.
  private def invoke(env: String, logType: String, start: String, end: String, to: String): VNelT[Unit] = {
    Apply[VNelT].apply5(validateEnvironment(env), validateLogType(logType),
      validateDateTime(start), validateDateTime(end),
      validateDestinationFile(to))((a, b, c, d, e) =>
      if (d.isAfter(c))
        extractLogs(s3, a, b, c, d, e)
      else Failure("Starting date must precede ending date").toValidationNel
    )
  }

  // Collect parameters
  val params =
    """--([A-Za-z0-9-]+)\s+([A-Za-z0-9/-]+)""".r

  def command(args: Array[String]): VNelT[Unit] = {
    val line = args.fold("")(_ + _)
    val tuples = params.findAllMatchIn(line).map(m =>
      if (2 == m.groupCount) (m.group(1), m.group(2)) else ("", "")).toSeq
    val map = Map(tuples: _*).withDefaultValue("")
    val env = map("env")
    val logType = map("log-type")
    val start = map("start")
    val end = map("end")
    val to = map("to")

    invoke(env, logType, start, end, to)
  }

}

