package com.hugolinton.query

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by hugol on 06/05/2017.
  */
@RunWith(classOf[JUnitRunner])
class ParquetQueryTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sparkContext : SparkContext = _
  var sqlContext : SQLContext = _

  //Set up Spark
  before{
    val sparkConf = new SparkConf().setAppName("ParquetQueryTest").setMaster("local[1]")
    sparkContext = SparkContext.getOrCreate(sparkConf)
    sqlContext = new SQLContext(sparkContext)
  }

  "Converting a PST to Parquet " should "work correctly and create a parquet file" in {
    val parquetPath = getClass.getResource("/").getPath

    val report = ParquetQuery.queryData(sqlContext, parquetPath)

    report.averageEmailLength should be (49)
    report.topRecipents.size should be (1)
    report.topRecipents.apply(0)._1 should be (3.5)
    report.topRecipents.apply(0)._2 should be ("hugolinton")
  }

}
