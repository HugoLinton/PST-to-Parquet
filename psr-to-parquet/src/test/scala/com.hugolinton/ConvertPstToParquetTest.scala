package com.hugolinton

import java.io.File

import com.hugolinton.utilities.TestUtilities
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by hugol on 06/05/2017.
  */
@RunWith(classOf[JUnitRunner])
class ConvertPstToParquetTest extends FlatSpec with Matchers with BeforeAndAfter{

  var sparkContext : SparkContext = _
  var sqlContext : SQLContext = _

  //Set up Spark
  before{
    val sparkConf = new SparkConf().setAppName("ConvertPstToParquetTest").setMaster("local[1]")
    sparkContext = SparkContext.getOrCreate(sparkConf)
    sqlContext = new SQLContext(sparkContext)
  }

  "Converting a PST to Parquet " should "work correctly and create a parquet file" in {
    val pstPath = getClass.getResource("/pst/email.pst").getPath
    val tmpDir = System.getProperty("user.dir") + "/pst2pTest"

    val parquetLocation = ConvertPstToParquet.pstToParquet(sparkContext,sqlContext,pstPath,tmpDir)

    TestUtilities.checkFileExists(parquetLocation, "_SUCCESS") should be (true)
    TestUtilities.checkFileExists(parquetLocation, ".parquet") should be (true)

    //Remove folder to clean up after test
    FileUtils.deleteDirectory(new File(tmpDir))
  }

  "Parquet file " should "contain the correct data from the PST file" in {
    val pstPath = getClass.getResource("/pst/email.pst").getPath
    val tmpDir = System.getProperty("user.dir") + "/pst2pTest"

    val parquetLocation = ConvertPstToParquet.pstToParquet(sparkContext,sqlContext,pstPath,tmpDir)

    val parquetData = sqlContext.read.parquet(parquetLocation)
    parquetData.count() should be (1)

    val email = parquetData.take(1).apply(0)
    email.getAs[String]("emailBody").replaceAll("(\\r|\\n)", "") should be ("Hello World! This is an email RegardsHugo Linton")
    email.getAs("sentTo").asInstanceOf[mutable.WrappedArray[String]].size should be (1)

    //Remove folder to clean up after test
    FileUtils.deleteDirectory(new File(tmpDir))
  }

}
