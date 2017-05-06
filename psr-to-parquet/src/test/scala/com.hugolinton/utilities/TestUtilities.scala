package com.hugolinton.utilities

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

/**
  * Created by hugol on 06/05/2017.
  */
object TestUtilities {

  def getListOfFiles(dir : String): List[File] = {
    val directory = new File(dir)
    if(directory.exists() && directory.isDirectory){
      directory.listFiles.filter(_.isFile).toList
    }else{
      List[File]()
    }
  }

  def checkFileExists(dir : String, fileName : String): Boolean = {
    val directory = new File(dir)
    if(directory.exists() && directory.isDirectory){
      directory.listFiles.exists(_.getName.contains(fileName))
    }else{
      false
    }

  }

  def getFileContent(path : String) : String = {
    val file = new File(path)
    val content = Source.fromFile(file).getLines().mkString
    content
  }

  def getFileSystem(path : String, fileName : String) = {
    val conf = new Configuration()
    val fs = FileSystem.getLocal(conf).getRawFileSystem
    fs.listStatus(new Path(path))
  }


}
