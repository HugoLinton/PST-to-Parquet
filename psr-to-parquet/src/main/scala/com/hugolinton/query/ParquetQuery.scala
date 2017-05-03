package com.hugolinton.query

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by hugol on 09/10/2016.
  */
object ParquetQuery {


  def main(args: Array[String]): Unit = {

    val parquetFolder = args(0)

    val sparkConf = new org.apache.spark.SparkConf().setAppName("org.sainsburys.test").setMaster("local");
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    queryData(sqlContext,parquetFolder)

  }

  def queryData(sqlContext : SQLContext, parquetDirectory : String) = {


    val folders = new java.io.File(parquetDirectory).listFiles
    val files = folders.map(folder => folder.listFiles.filter(_.getName.endsWith(".parquet")))

    val parquetList = files.flatMap(file => file.map(f => sqlContext.parquetFile(f.getPath)))
    val mergedParquet = parquetList.reduce((x, y) => x.unionAll(y))

    mergedParquet.registerTempTable("emailData")

    val averageEmailLengthColumn = sqlContext.sql("SELECT length(emailBody) as email_length FROM emailData").agg(sum("email_length"))
    val totalEmailCharacters = averageEmailLengthColumn.take(1).map(row => row.getLong(0))

    val sentToEmailDf = sqlContext.sql("SELECT sentTo FROM emailData")
    val sentEmails = sentToEmailDf.flatMap(row => row.getAs[Seq[String]](0))
    val mostSentTo = sentEmails.map(e => (e, 1.0)).reduceByKey((hc1, hc2) => hc1 + hc2)

    val ccToEmailDf = sqlContext.sql("SELECT ccTo FROM emailData")
    val ccEmails = ccToEmailDf.flatMap(row => row.getAs[Seq[String]](0))
    val mostccTo = ccEmails.map(e => (e, 0.5)).reduceByKey((hc1, hc2) => hc1 + hc2)

    val mergedRecipents = mostSentTo.join(mostccTo).map({
      case (user, (sent, cc)) => (user, (sent + cc))
    })

    val sorted = mergedRecipents.map { case (email, count) => (count, email) }.sortByKey(ascending = false).collect



    try {
      var record = 0
      val averageEmailLength = totalEmailCharacters.apply(0) / mergedParquet.count()
      println("Email Average Length is: " + averageEmailLength)
      println("Top 150 Recipents: ")
      sorted.take(150).foreach(recipents => {
        println("Record " + record + ": " + recipents)
        record += 1
      })
    } catch {
      case e: ClassCastException => error("Total not a Long")
      case ofb: IndexOutOfBoundsException => error("Df most likely empty(Index out of bounds)")
    }
  }
}
