package com.hugolinton.model

/**
  * Created by hugol on 30/05/2017.
  */
case class Transaction(
                        transactionId: String,
                        accountId: String,
                        transactionDay: Int,
                        category: String,
                        transactionAmount: Double)

