package org.apache.spark.examples.streaming

// Sep 24 21:06:01 interset1 rsyslogd-3000: unknown priority name "" [try http://www.rsyslog.com/e/3000 ]

case class SysLogRecord(dateTime:String , client:String , messageID:String , messageString:String )

/**
  * Created by dmilan on 9/25/17.
  */


  import java.util.regex.Pattern
  import java.text.SimpleDateFormat
  import java.util.Locale
  import scala.util.control.Exception._
  import java.util.regex.Matcher
  import scala.util.{Try, Success, Failure}
  @SerialVersionUID(100L)
  class SysLogParser extends Serializable {
    // Sep 24 21:06:01 interset1 rsyslogd-3000: unknown priority name "" [try http://www.rsyslog.com/e/3000 ]

    private val dateTime = "(\\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2})"              // like `Sep 24 21:06:01`
    private val client = "(\\S+)"                     // '\S' is 'non-whitespace character'
    private val messageId = "(\\S+)"
    private val messageString = "(.*)"                 // any number of any character, reluctant

    private val regex = s"$dateTime $client $messageId $messageString"

    println(regex)
    private val p = Pattern.compile(regex)

    def parseRecord(record: String): SysLogRecord = {
      val newRecord = if(record!=null) record.trim else record
      val matcher = p.matcher(newRecord)
      if (matcher.find) {
        buildSysLogParser(matcher)
      } else {
        SysLogRecord("BAD", "BAD", "BAD", "BAD")
      }
    }

    def parseRecordReturningNullObjectOnFailure(record: String): SysLogRecord = {
      val matcher = p.matcher(record)
      if (matcher.find) {
        buildSysLogParser(matcher)
      } else {
        SysLogParser.nullObjectSysLogParser
      }
    }

    private def buildSysLogParser(matcher: Matcher):SysLogRecord = {
      SysLogRecord(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        matcher.group(4)
   )
    }
  }
  object SysLogParser {

    val nullObjectSysLogParser = SysLogRecord("", "", "", "")

    def parseFromLogLine(request: String): SysLogRecord ={
      new SysLogParser().parseRecord(request)
    }

  }
