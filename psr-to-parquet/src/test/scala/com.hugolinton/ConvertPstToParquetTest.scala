package com.hugolinton

import java.io.File

import com.hugolinton.utilities.TestUtilities
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

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
    sparkContext = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sparkContext)
  }

  "Converting a PST to Parquet " should "work correctly and create a parquet file" in {
    val pstPath = getClass.getResource("/pst/email.pst").getPath
    val tmpDir = System.getProperty("user.dir") + "/pst2pTest"

    val parquetLocation = ConvertPstToParquet.pstToParquet(sparkContext,sqlContext,pstPath,tmpDir)

    val testFiles = TestUtilities.getListOfFiles(parquetLocation)
    TestUtilities.checkFileExists(parquetLocation, "_SUCCESS") should be (true)
    TestUtilities.checkFileExists(parquetLocation, ".parquet") should be (true)

    //Remove folder to clean up after test
    FileUtils.deleteDirectory(new File(tmpDir))
  }

}
