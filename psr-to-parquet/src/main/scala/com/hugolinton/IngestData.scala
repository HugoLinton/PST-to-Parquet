package com.hugolinton

import com.databricks.spark.csv.CsvParser
import com.hugolinton.model.EmailModel
import com.hugolinton.query.ParquetQuery
import com.hugolinton.ziptools.ZipHelper
import com.pff.{PSTFile, PSTMessage}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

/**
  * Created by hugol on 08/10/2016.
  */
object IngestData {

  //Just to run Locally
//  System.setProperty("hadoop.home.dir", "D:\\Hadoop\\winutils\\hadoop-2.6.0")

  val schemaString = "id emailBody sentTo ccTo"
  val schema = StructType(Array(StructField("id", StringType, false),
                          (StructField("emailBody", StringType, false)),
                          (StructField("sentTo", ArrayType(StringType))),
                          (StructField("ccTo", ArrayType(StringType)))
  ))

  def main(args: Array[String]): Unit = {

    val inputFolder = args(0)
    val parquetFolder = args(1)

    val sparkConf = new org.apache.spark.SparkConf().setAppName("org.sainsburys.test").setMaster("local");
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    pstToParquet(sparkContext,sqlContext,inputFolder,parquetFolder)

    ParquetQuery.queryData(sqlContext,parquetFolder)

  }

  def pstToParquet(sparkContext : SparkContext,sqlContext : SQLContext, filePath : String, outputFolder : String)= {

    val pstFile = new PSTFile("/data/emailData/"+filePath)
    val expandedPSTFiles = new ExpandPSTFiles()
    expandedPSTFiles.processFolder(pstFile.getRootFolder)
    val emails = expandedPSTFiles.getEmails.toSet.map( (e: PSTMessage) => {
      EmailModel.pstToModel(e)
    }).toSeq

    if(emails.size != expandedPSTFiles.getEmails.size()){
      error("Some records were lost!")
    }

    val rdd = sparkContext.parallelize(emails).map(email => Row(email.id, email.emailBody, email.sentTo, email.ccTo))

    val df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet(outputFolder +System.currentTimeMillis.toString)

  }
}
