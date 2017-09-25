import java.util.regex.Pattern

import org.apache.spark.examples.streaming.{SysLogParser, SysLogRecord}

private val dateTime = "(\\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2})"              // like `Sep 24 21:06:01`
private val client = "(\\S+)"                     // '\S' is 'non-whitespace character'
private val messageId = "(\\S+)"
private val messageString = "(.*)"                 // any number of any character, reluctant

private val regex = s"$dateTime $client $messageId $messageString"
println(regex)

private val p = Pattern.compile(regex)




val note = "Sep 25 17:07:39 interset1 test: Hello SparkStreaming 144"
val matcher = p.matcher(note)
if (matcher.find) {
  println("matcher.group(1): "+matcher.group(1))
  println("matcher.group(2): "+matcher.group(2))
  println("matcher.group(3): "+matcher.group(3))
  println("matcher.group(4): "+matcher.group(4))

  SysLogRecord(
    matcher.group(1),
    matcher.group(2),
    matcher.group(3),
    matcher.group(4)
  )
} else {
  SysLogRecord("BAD", "BAD", "BAD", "BAD")
}

val sysLogRecord = SysLogParser.parseFromLogLine(note)