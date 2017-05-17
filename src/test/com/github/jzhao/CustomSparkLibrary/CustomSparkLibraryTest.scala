package test.com.github.jzhao.CustomSparkLibrary

/**
  * Created by jzhao on 5/17/2017.
  */
import main.com.github.jzhao.CustomSparkLibrary.CustomLibrary._
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
}
