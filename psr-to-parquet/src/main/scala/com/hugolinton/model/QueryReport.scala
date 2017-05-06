package com.hugolinton.model

/**
  * Created by hugol on 06/05/2017.
  */
case class QueryReport (averageEmailLength : Long, topRecipents : Array[(Double,String)]) extends Product
