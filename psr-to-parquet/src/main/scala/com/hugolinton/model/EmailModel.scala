package com.hugolinton.model

import java.util.UUID

import com.pff.PSTMessage
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
  * Created by hugol on 09/10/2016.
  */
case class EmailModel (id : String, emailBody : String, sentTo : Array[String], ccTo : Array[String]) extends Product

object EmailModel {

  final val schema = StructType(Array(StructField("id", StringType, false),
    (StructField("emailBody", StringType, false)),
    (StructField("sentTo", ArrayType(StringType))),
    (StructField("ccTo", ArrayType(StringType)))
  ))

  def pstToModel(pst : PSTMessage) = {
    val uniqueID = UUID.randomUUID().toString();
    val to = pst.getDisplayTo.replaceAll("""[.,\/#!$%\^&\*;:{}=\-_`~()\s]""", "").toLowerCase.split(";")
    val cc = pst.getDisplayCC.replaceAll("""[.,\/#!$%\^&\*;:{}=\-_`~()\s]""", "").toLowerCase.split(";")
    EmailModel(uniqueID,safeGet(pst.getBody),to,cc)
  }


  def safeGet(field : String) : String = {
    if(field.isEmpty || field == null){
      return ""
    }
    field
  }
}
