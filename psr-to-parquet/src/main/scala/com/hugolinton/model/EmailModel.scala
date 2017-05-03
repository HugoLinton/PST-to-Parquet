package com.hugolinton.model

import java.util.UUID

import com.pff.PSTMessage
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StructField}

/**
  * Created by hugol on 09/10/2016.
  */
case class EmailModel (id : String, emailBody : String, sentTo : Array[String], ccTo : Array[String]) extends Product

object EmailModel {

  var totalEmails = 0
  var length = 0

  def pstToModel(pst : PSTMessage) = {
    totalEmails += 1
    length = length + pst.getBody.length
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
