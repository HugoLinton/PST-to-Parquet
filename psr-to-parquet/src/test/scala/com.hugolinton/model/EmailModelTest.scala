package com.hugolinton.model

import com.hugolinton.ExpandPSTFiles
import com.pff.{PSTFile, PSTMessage}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConversions._

/**
  * Created by hugol on 06/05/2017.
  */
@RunWith(classOf[JUnitRunner])
class EmailModelTest extends FlatSpec with Matchers with BeforeAndAfter {

  "Converting a PST to Parquet " should "work correctly and create a parquet file" in {
    val pstPath = getClass.getResource("/pst/email.pst").getPath
    val pstFile = new PSTFile(pstPath)

    val expandedPSTFiles = new ExpandPSTFiles()
    expandedPSTFiles.processFolder(pstFile.getRootFolder)
    val emails = expandedPSTFiles.getEmails.toSeq.map((e: PSTMessage) => {
      EmailModel.pstToModel(e)
    })

    val email = emails.apply(0)
    email.emailBody.replaceAll("(\\r|\\n)", "") should be ("Hello World! This is an email RegardsHugo Linton")
    email.sentTo.size should be (1)
    email.sentTo.contains("hugolinton") should be (true)
    email.ccTo.contains("") should be (true)
  }

}
