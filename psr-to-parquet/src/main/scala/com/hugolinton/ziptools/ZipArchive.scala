package com.hugolinton.ziptools

import com.hugolinton.UnzipUtils
import org.apache.commons.io.FileUtils

import scala.collection.JavaConversions._


/**
  * Created by hugol on 09/10/2016.
  */
object ZipHelper {

  def getPstFilePaths(compressZip : String, outputFolder : String): List[String] = {
    println(compressZip)
    val extractedFiles = new UnzipUtils().unzipFile(compressZip,outputFolder).toList
    extractedFiles
  }

  def removeFiles(files : List[String]) = {
    files.foreach(path => {
      if(FileUtils.deleteQuietly(new java.io.File(path))){
        println("Removed File: " + path)
      }else{
        println("Failed To Removed File: " + path)
      }
    })
  }

  //Doesn't handle Subdirectories
  def getZipFiles(format : String, path : String):  List[String] =  {
    println(path)
    val folders = new java.io.File(path).listFiles
    val matchingFiles = folders.filter(_.getName.endsWith(".zip")).filter(_.getName.toLowerCase.contains(format)).toList
    val files = matchingFiles.map(file => file.getPath)
    files
  }

  def getPstFiles(path : String):  List[String] =  {
    println(path)
    val folders = new java.io.File(path).listFiles
    val matchingFiles = folders.filter(_.getName.endsWith(".pst")).toList
    val files = matchingFiles.map(file => file.getPath)
    files
  }

}
