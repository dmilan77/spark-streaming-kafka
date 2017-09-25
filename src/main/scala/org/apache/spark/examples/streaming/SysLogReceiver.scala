package org.apache.spark.examples.streaming

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  *
  * Usage: SysLogReceiver <hostname> <port>
  * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example org.apache.spark.examples.streaming.SysLogReceiver localhost 9999`
  */
object SysLogReceiver {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SysLogReceiver <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))


    val sqlContext = new SQLContext(sc)
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // lines.foreachRDD(println(_))

    //
    //    lines.foreachRDD(
    //
    //      rdd=>
    //        rdd.map((record:String)=>{
    //        val sysLogRecord:SysLogRecord = SysLogParser.parseFromLogLine(record)
    //        println("****************************")
    //        println(sysLogRecord.messageString)
    //        println(sysLogRecord.dateTime)
    //        println(sysLogRecord.client)
    //        println(sysLogRecord.messageID)
    //        println("****************************")
    //
    //        sysLogRecord
    //      }
    //      )
    //    )

    //    lines.foreachRDD(rdd=>
    //      rdd.map( (record:String) => (SysLogParser.parseFromLogLine(record)) )
    //
    //    )
    //
    //
    //    lines.foreachRDD(rdd => {
    //
    //      val sysLogRDD = rdd.map((record: String) => {
    //        println("*************************** RDD  ********  " + record)
    //        SysLogParser.parseFromLogLine(record)
    //
    //      })
    //      val sysLogDF = sqlContext.createDataFrame(sysLogRDD)
    //
    //      sysLogDF.write.mode(SaveMode.Overwrite).parquet("/user/hive/syslog/v1/syslogrecord")
    //    }
    //
    //    )
    //


    //val mapStream = lines.map(event => new String(event.event.getBody().array(), "UTF-8"))
    val mapStream = lines.map(event => new String(event.getBytes(), "UTF-8"))

    mapStream.foreachRDD(rdd => {

      val sysLogRDD = rdd.map((record: String) => {
        println("*************************** RDD  ********  " + record)
        val sysLogRecord = SysLogParser.parseFromLogLine(record)
        println("********** ******* .." + sysLogRecord)

        //val sysLogDF = sqlContext.createDataFrame(sc.parallelize(List(sysLogRecord)))
        //sysLogDF.write.mode(SaveMode.Overwrite).parquet("/user/hive/syslog/v1/syslogrecord")
        sysLogRecord

      })
      //      println("******** printing sysLogRDD *****************")
      //      println(sysLogRDD.collect())
      //
      val sysLogDF = sqlContext.createDataFrame(sysLogRDD)

      sysLogDF.write.mode(SaveMode.Append).parquet("/user/hive/syslog/v1/syslogrecord")
      //sysLogDF.write.mode(SaveMode.Overwrite).text("/user/hive/syslog/v1/syslogrecordtext")
      //      println("******** printing sysLogDF *****************")
      //      println(sysLogDF.collect())

    }

    )

    //val accessLogDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line)).cache()

    // val arr = lines.toString.split("\n").filter(_ != "")

    //val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    lines.print()
    //wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

// scalastyle:on println