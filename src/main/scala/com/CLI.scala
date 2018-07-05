package com

import java.io.{BufferedWriter, File, FileWriter, Writer}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.util.Filtering._
import com.util.S3ObjectIterator
import com.util.S3ObjectIterator.{asJsObjects, getContentAsString}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import scalaz.Validation._
import scalaz.{Apply, Failure, NonEmptyList, Success, ValidationNel}

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
  def validateDateTime(str: String): ValidationNel[String, DateTime] = fromTryCatchNonFatal(
    DateTime.parse(str, formatter)
  ).leftMap(_ => s"${emptyOrInvalid(str, s"date/time: ${str}")}, format is $dateTimePattern ").toValidationNel

  def validateDateRange(start: String, end: String): ValidationNel[String, (DateTime, DateTime)] = {
    val v1 = validateDateTime(start)
    val v2 = validateDateTime(end)
    (v1, v2) match {
      case (Failure(e1), Failure(e2)) => Failure(e1.append(e2))
      case (Failure(e1), Success(_)) => Failure(e1)
      case (Success(_), Failure(e2)) => Failure(e2)
      case (Success(d1), Success(d2)) =>
        if (d2.isAfter(d1) || d1 == d2) Success(d1 -> d2) else
          Failure("Ending date precedes starting date").toValidationNel
    }
  }

  def validateEnvironment(str: String): ValidationNel[String, String] =
    (if (environments.contains(str)) Success(str) else
      Failure(s"${emptyOrInvalid(str, "environment")}, should be one of ${environments.mkString(", ")}")).toValidationNel

  def validateLogType(str: String): ValidationNel[String, LogType] =
    (if (logTypes.keySet.contains(str)) Success(logTypes(str)) else
      Failure(s"${emptyOrInvalid(str, "log type")}, should be one of ${logTypes.keySet.mkString(", ")}")).toValidationNel

  def validateDestinationFile(str: String): ValidationNel[String, File] = fromTryCatchNonFatal {
    val file = new File(str)
    file.createNewFile()
    file
  }.leftMap(_ => s"${emptyOrInvalid(str, "file path")}").toValidationNel


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
        // Generate all the prefixes for the specified range. YYYY/MM/dd/HH...
        val ps = prefixes(env, start, end)
        ps.foreach(p => {
          val s3Iter = S3ObjectIterator(s3, "cda_logs", p)
          // Iterate through all of the objects in that folder.
          while (s3Iter.hasNext) {
            val s = getContentAsString(s3Iter.next())
            // Transform the data messages in the selected log to JSON objects.
            val jsObjects = asJsObjects(s).filter(jsObject => {
              val p = for {
                b1 <- isDataMessage(jsObject)
                b2 <- matchLogType(jsObject, logType)
              } yield b1 && b2
              p.getOrElse(false)
            })

            // Write the entries to a File.
            writer.write(jsObjects.map(Json.prettyPrint).mkString("\n", "\n", "\n"))
          }
        })
      }
    )
  }

  type VNelT[T] = ValidationNel[String, T]

  // Extract log content if parameters validate.
  private def invoke(env: String, logType: String, start: String, end: String, to: String): VNelT[Unit] =
    Apply[VNelT].apply4(validateEnvironment(env), validateLogType(logType),
      validateDateRange(start, end), validateDestinationFile(to))((a, b, c, d) =>
      extractLogs(s3, a, b, c._1, c._2, d))

  // Collect parameters
  val params =
    """--([A-Za-z0-9-]+)(\s+([A-Za-z0-9/.-]+))?""".r

  val usage = "Usage: extract-logs --env <environment> --log-type <log-type> --start <start-date>  --end <end-date> --to <file-name>"

  def command(args: Array[String]): VNelT[Map[String, String]] = {

    // Build a Map containing all command line options: --env dev, --to data/logs.txt,  etc.
    val tuples = params.findAllMatchIn(args.mkString(" ")).map(m =>
      if (3 == m.groupCount) (m.group(1), m.group(3)) else ("", "")).toSeq
    val map = Map(tuples: _*).withDefaultValue("")
    val env = map("env")
    val logType = map("log-type")
    val start = map("start")
    val end = map("end")
    val to = map("to")

    if (map.get("help").isDefined) Failure(NonEmptyList(usage)) else
      invoke(env, logType, start, end, to).map(_ => map)

  }

}

