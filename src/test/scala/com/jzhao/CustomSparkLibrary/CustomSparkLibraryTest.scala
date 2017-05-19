package com.jzhao.CustomSparkLibrary

/**
  * Created by jzhao on 5/17/2017.
  */

import com.amazonaws.auth.{EnvironmentVariableCredentialsProvider}
import com.jzhao.CustomSparkLibrary.CustomLibrary._
import org.scalatest.FunSuite

class CustomSparkLibraryTest extends FunSuite{

  val prepareData = PrepareDataFromS3("table.csv")
  val prepareDataWithBucket = PrepareDataFromS3(fileName = "table.csv", bucket = "snowf0xrawdata")

  test("data row count"){
    assert(prepareData.toDF().count() == 11338)
  }

  test("data column name check"){
    assert(prepareData.toDF().columns.mkString(",") == "Date,Open,High,Low,Close,Volume,Adj Close")
  }

  test("metadata test"){
    assert(prepareData.meta.fileName == "table.csv")
  }

  test("apply new metadata test"){
    val dataWithNewMeta = prepareData.getAndApplyMetaData("new file name")
    assert(dataWithNewMeta.meta.fileName == "new file name")
  }

  test("zeppelin chart print"){
    assert(Util.zeppelinVisualize(prepareData.toDF()).split('\n')(0)=="%table Date\tOpen\tHigh\tLow\tClose\tVolume\tAdj Close")
  }
  test("data with bucket row count"){
    assert(prepareDataWithBucket.toDF().count() == 11338)
  }

  test("amazon s3 mapper"){
    import com.amazonaws.services.s3.AmazonS3ClientBuilder

    val s3Client = AmazonS3ClientBuilder.standard.withCredentials(new EnvironmentVariableCredentialsProvider()).withRegion("us-west-2").build

    val fileOwnerTuples = Util.listS3files(s3Client,"snowf0xrawdata","t")(s => (s.getKey, s.getOwner))

    assert(fileOwnerTuples(0)._1 == "table.csv")
  }
}
