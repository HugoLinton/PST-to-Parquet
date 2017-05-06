package com.hugolinton

import com.hugolinton.model.EmailModel
import com.pff.{PSTFile, PSTMessage}
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

/**
  * Created by hugol on 08/10/2016.
  */
object ConvertPstToParquet extends Logging {

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      Console.err.println("Usage: ConvertPstToParquet <PST File Path> <Export Folder><Hadoop Home Directory>")
    }

    val Array(pstFile, parquetFolder, hadoopHome) = args

    System.setProperty("hadoop.home.dir", hadoopHome)

    val sparkConf = new org.apache.spark.SparkConf().setAppName("com.hugolinton.pst-to-parquet").setMaster("local");
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    pstToParquet(sparkContext,sqlContext,pstFile,parquetFolder)
  }

  def pstToParquet(sparkContext : SparkContext,sqlContext : SQLContext, filePath : String, outputFolder : String) : String = {

    val pstFile = new PSTFile(filePath)
    val expandedPSTFiles = new ExpandPSTFiles()
    expandedPSTFiles.processFolder(pstFile.getRootFolder)
    val emails = expandedPSTFiles.getEmails.toSet.map( (e: PSTMessage) => {
      EmailModel.pstToModel(e)
    }).toSeq

    if(emails.size != expandedPSTFiles.getEmails.size()){
      error("Some records were lost!")
    }

    val rdd = sparkContext.parallelize(emails).map(email => Row(email.id, email.emailBody, email.sentTo, email.ccTo))

    val df = sqlContext.createDataFrame(rdd, EmailModel.schema)
    val exportLocation = outputFolder + "\\" + System.currentTimeMillis.toString
    info("Export Location: " + exportLocation)
    df.write.parquet(exportLocation)
    exportLocation
  }
}
