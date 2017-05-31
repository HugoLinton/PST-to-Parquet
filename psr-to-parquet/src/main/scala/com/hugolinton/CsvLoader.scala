package com.hugolinton

import com.hugolinton.model.{ReportItem, Transaction}
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


    println("Calculate the Total Transactions per day")
    val transactions = getTransactions(csvFilePath)
    getTransactionsGroupedByDay(transactions)


    println("Calculate the average value of transactions per account for each type of transaction")
    getAccountAverage(transactions)

    getAccountStatistics(transactions)
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

  def getAccountStatistics( transactions : List[Transaction]) = {
    val groupedByAccount = transactions.groupBy(trans => trans.accountId)
    groupedByAccount.map(account => {
      val days = account._2.sortBy(-_.transactionDay)
      val maxDay = transactions.reduceLeft(ReportItem.maxTransactionDay).transactionDay + 1
      val dayRange = List.range(1,maxDay)
      dayRange.foreach(day => createDayReport(days,day))
    })
  }

  def createDayReport(transactions : List[Transaction], reportDay : Int) : List[Transaction]  = {
    val results = transactions.filter(day => transactions.head.transactionDay - 5 < day.transactionDay && reportDay != day.transactionDay)
    val report = getReportString(results)
    println(report)
    removeDuplicates(transactions, transactions.head)
  }

  def removeDuplicates(transactions : List[Transaction], duplicate : Transaction) : List[Transaction] = {
    transactions.filter(day => day.transactionDay != duplicate.transactionDay)
  }

  def printMap(values : Map [String, Double]) = {
    val sortedValues = values.toSeq.sortBy(_._1)
    sortedValues.foreach(value => println(value.toString()))
  }



  def getReportString(transactions : List[Transaction]) : String = {
    if (transactions.size > 0) {
      val headTransaction = transactions.head
      val maxTransaction = transactions.reduceLeft(ReportItem.maxTransactionAmount).transactionAmount
      val avg = transactions.map(_.transactionAmount).sum / transactions.size
      val categories = transactions.groupBy(x => x.category).filter(category => category._1 == "AA"
        || category._1 == "CC"
        || category._1 == "FF").map(x => (x._1, x._2.map(_.transactionAmount).sum))


      "Day: " + headTransaction.transactionDay +
        " Account ID: " + headTransaction.accountId +
        " Maximum: " + maxTransaction +
        " Average: " + avg +
        " AA Total Value: " + categories.get("AA").getOrElse("N/A") +
        " CC Total Value: " + categories.get("CC").getOrElse("N/A") +
        " FF Total Value: " + categories.get("FF").getOrElse("N/A")
    }else {
      ""
    }
  }

}
