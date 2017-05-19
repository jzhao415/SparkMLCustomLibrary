package com.jzhao.CustomSparkLibrary

/**
  * Created by jzhao on 5/17/2017.
  */

import com.jzhao.CustomSparkLibrary.CustomLibrary._
import org.scalatest.FunSuite

class CustomSparkLibraryTest extends FunSuite{

  val prepareData = PrepareDataFromS3().setOption(value = "snowf0xrawdata")
  val fp = prepareData.getFileAsPackage("table.csv")
  val prepareDataDF = prepareData.getFileAsDF("table.csv")

  test("data row count"){
    assert(prepareDataDF.count() == 11338)
  }

  test("data column name check"){
    assert(prepareDataDF.columns.mkString(",") == "Date,Open,High,Low,Close,Volume,Adj Close")
  }

  test("apply new metadata test"){
    val dataWithNewMeta = prepareData.getFileAsPackage("table.csv")
    assert(dataWithNewMeta.meta.fileName == "table.csv")
  }

  test("zeppelin chart print"){
    assert(LibraryUtil.zeppelinVisualize(prepareDataDF).split('\n')(0)=="%table Date\tOpen\tHigh\tLow\tClose\tVolume\tAdj Close")
  }

  test("amazon s3 mapper"){
    assert(prepareData.listFiles("table") == List("table.csv"))
  }
}
