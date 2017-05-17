package test.com.github.jzhao.CustomSparkLibrary

/**
  * Created by jzhao on 5/17/2017.
  */
import main.com.github.jzhao.CustomSparkLibrary.CustomLibrary._
import org.scalatest.FunSuite

class CustomSparkLibraryTest extends FunSuite{
  val preparedata = PrepareDataFromS3("table.csv").getAndApplyMetaData("metadata id 123")
  preparedata.toDF().show()

  test("data row count"){
    assert(preparedata.toDF().count() == 11338)
  }

  test("data column name check"){
    assert(preparedata.toDF().columns.mkString(",") == "Date,Open,High,Low,Close,Volume,Adj Close")
  }
}
