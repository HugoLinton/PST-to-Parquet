package com.hugolinton

import com.hugolinton.model.Transaction
import grizzled.slf4j.Logging

import scala.io.Source

/**
  * Created by hugol on 30/05/2017.
  */
object CsvLoader extends Logging {

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      Console.err.println("Usage: ParquetQuery <CSV File Path>")
    }

    val Array(csvFilePath) = args
//
//    val sparkConf = new org.apache.spark.SparkConf().setAppName("hugo-linton-csv").setMaster("local");
//    val sparkContext = new SparkContext(sparkConf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
//
//    val df = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true") // Use first line of all files as header
//      .option("inferSchema", "true") // Automatically infer data types
//      .load(csvFilePath)
//
//
////    val q1 = getTransactionsGroupedByDay(df)
////    q1.show(31)
//
//    val q2 =getAccountAverage(df)
//    q2.show(1000)


    println("Calculate the Total Transactions per day")
    val transactions = getTransactions(csvFilePath)
    getTransactionsGroupedByDay(transactions)


    println("Calculate the average value of transactions per account for each type of transaction")
    getAccountAverage(transactions)
  }

  def getTransactions(path : String) : List[Transaction]= {
    val transactionsLines = Source.fromFile(path).getLines().drop(1)

    transactionsLines.map { line =>
      val split = line.split(',')
      Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
    }.toList
  }


    def getTransactionsGroupedByDay( transactions : List[Transaction]) = {
      val results = transactions.groupBy(trans => trans.transactionDay).map(x => (x._1.toString,x._2.map(_.transactionAmount).sum))
      printMap(results)
      results
    }

    def getAccountAverage( transactions : List[Transaction]) = {
      val groupedByAccount = transactions.groupBy(trans => trans.accountId)
      groupedByAccount.map(account => {
        val categories = account._2.groupBy(x => x.category).map(x => (x._1,x._2.map(_.transactionAmount).sum / x._2.size))
        println("--------- " + account._1 + " ---------")
        printMap(categories)
        (account._1, categories)
      })
    }

  def printMap(values : Map [String, Double]) = {
    val sortedValues = values.toSeq.sortBy(_._1)
    sortedValues.foreach(value => println(value.toString()))
  }

//  def getTransactionsGroupedByDay( dataframe : DataFrame) = {
//    dataframe.groupBy("transactionDay").agg(sum("transactionAmount"))
//  }
//
//  def getAccountAverage( dataframe : DataFrame) = {
//    dataframe.groupBy("accountId","category").agg(mean("transactionAmount"))
//  }

}
