package com.util

import org.joda.time.{DateTime, DateTimeZone}

import scala.language.implicitConversions

/*
  Utility to generate date based bucket keys for log archives. Keys are of the format:
  <year>/<month>/<day>/<hour>, i.e. 2018/06/29/13
 */


object PrefixUtil {
  implicit def mkOps[A](x: A)(implicit ord: math.Ordering[A]): ord.Ops =
    ord.mkOrderingOps(x)

  def months: List[Int] = (1 to 12).toList

  def days(year: Int, month: Int): List[Int] = {
    val dateTime = new DateTime().withYear(year).withMonthOfYear(month)
    val range = 1 to dateTime.dayOfMonth().getMaximumValue
    range.toList
  }

  def hours: List[Int] = (0 to 23).toList

  // Generate all the Int values for the prefixes for a range of years
  def prefixValues(years: List[Int]): List[(Int, Int, Int, Int)] = for {
    y <- years
    m <- months
    d <- days(y, m)
    h <- hours
  } yield (y, m, d, h)

  // Generate prefix values for the specified environment for the given range inclusive
  def prefixes(env: String, start: DateTime, end: DateTime): List[String] = {
    // First put the range in UTC
    val sd = start.withZone(DateTimeZone.UTC)
    val ed = end.withZone(DateTimeZone.UTC)
    val s = (sd.getYear, sd.getMonthOfYear, sd.getDayOfMonth, sd.getHourOfDay)
    val e = (ed.getYear, ed.getMonthOfYear, ed.getDayOfMonth, ed.getHourOfDay)
    // Filter out values not in the range
    val tuples = prefixValues((sd.getYear to ed.getYear).toList).filter(p => p >= s && p <= e)
    // Convert to Strings
    tuples.map { case (y, m, d, h) => f"$env/$y/$m%02d/$d%02d/$h%02d/" }
  }
}
