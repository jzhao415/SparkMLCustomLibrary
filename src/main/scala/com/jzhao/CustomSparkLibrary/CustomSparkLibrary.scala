package com.jzhao.CustomSparkLibrary

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import java.util.Calendar;

object CustomLibrary{

  case class MetaData(fileName:String, accessTimeStamp: Timestamp){}

  object MetaData{
    def apply(fileName:String):MetaData = MetaData(fileName,
      new java.sql.Timestamp(Calendar.getInstance().getTime().getTime()))
  }

  val spark = SparkSession.builder().appName("CustomLibraryPackage").config("spark.master", "local").getOrCreate()

  //Here I assume you already have AWS credential setup in someway, such as core-site.xml
  //if you do not have AWS credential setup, uncomment following two lines and fill in your own aws user Access Key and secrete

  //spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "[your own access key]")
  //spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "[your own access secrete]")

  trait PrepareData[T,X]{
    def getAndApplyMetaData(meta: T): X
    def toDF():DataFrame
    def zeppelinChart():Unit
  }

  case class PrepareDataFromS3(dataDF: DataFrame, meta: MetaData) extends PrepareData[String, PrepareDataFromS3]{

    override def getAndApplyMetaData(metaFile: String): PrepareDataFromS3 = copy(dataDF, meta = MetaData(metaFile))
    override def toDF():DataFrame = dataDF
    override def zeppelinChart() = Util.zeppelinVisualize(dataDF)

    def apply(fileName: String):DataFrame = {
      Util.getS3file(bucket="snowf0xrawdata",fileName, true, true)
    }
  }

  object PrepareDataFromS3{
    def apply(fileName: String, bucket: String = "snowf0xrawdata"):PrepareDataFromS3 = PrepareDataFromS3(
      Util.getS3file(bucket,fileName),
      MetaData(fileName)
    )
  }

  object Util{
    def getS3file(bucket: String, fileName: String, header: Boolean = true, inferSchema: Boolean = true):DataFrame= spark.read.option("header", header).option("inferSchema", inferSchema).csv("s3n://"+bucket+"/"+fileName)
    def zeppelinVisualize(dataDF:DataFrame):String = {
      var chart = ("%table "+dataDF.columns.mkString("\t")+"\n")
      dataDF.collect().foreach(x=>chart = chart.concat(x.mkString("\t")+"\n"))
      chart
    }
  }
}
