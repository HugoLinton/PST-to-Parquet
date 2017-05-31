package com.hugolinton.model

import grizzled.slf4j.Logging

/**
  * Created by hugol on 30/05/2017.
  */
case class ReportItem(
                        day: Int,
                        accountId: String,
                        maximum: Double,
                        average: Double,
                        aaTotal: Option[Double],
                        ccTotal: Option[Double],
                        ffTotal: Option[Double])

object ReportItem extends Logging {

  def maxTransactionAmount(t1: Transaction, t2: Transaction): Transaction = {
    if (t1.transactionAmount > t2.transactionAmount) t1 else t2
  }

  def maxTransactionDay(t1: Transaction, t2: Transaction): Transaction = {
    if (t1.transactionDay > t2.transactionDay) t1 else t2
  }


}
