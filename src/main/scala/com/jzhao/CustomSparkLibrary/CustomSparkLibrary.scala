package com.jzhao.CustomSparkLibrary

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import java.util.Calendar

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3ClientBuilder}
import com.jzhao.CustomSparkLibrary.LibraryUtil._

object CustomLibrary{

  case class MetaData(fileName:String, accessTimeStamp: Timestamp){}

  case class FilePackage(dataDF: DataFrame, meta: MetaData){
    def showZeppelinChart() = LibraryUtil.zeppelinVisualize(dataDF)
  }

  object MetaData{
    def apply(fileName:String):MetaData = MetaData(fileName,
      new java.sql.Timestamp(Calendar.getInstance().getTime().getTime()))
  }

  val spark = SparkSession.builder().appName("CustomLibraryPackage").config("spark.master", "local").getOrCreate()
  val s3Client = AmazonS3ClientBuilder.standard.withCredentials(new EnvironmentVariableCredentialsProvider()).withRegion("us-west-2").build

  //Here I assume you already have AWS credential setup in someway, such as core-site.xml
  //if you do not have AWS credential setup, uncomment following two lines and fill in your own aws user Access Key and secrete

  //spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "[your access key]")
  //spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "[your access secrete]")

  trait PrepareData[T,X]{
    def getFileAsDF(fileName: T): DataFrame
    def listFiles(prefix: T): List[String]
    def getFileAsPackage(fileName:T): FilePackage
    def setOption(optionName: T, value: T):X
  }

  case class PrepareDataFromS3(bucket: String) extends PrepareData[String, PrepareDataFromS3] {

    override def getFileAsDF(fileName: String): DataFrame = LibraryUtil.getS3file(spark, bucket, fileName)

    override def listFiles(prefix: String): List[String] = LibraryUtil.listS3files(s3Client, bucket, prefix)(s => (s.getKey))

    override def getFileAsPackage(fileName: String): FilePackage = FilePackage(LibraryUtil.getS3file(spark, bucket, fileName), MetaData(fileName))

    override def setOption(optionName:String = "bucket", value: String): PrepareDataFromS3 = copy(bucket = value)
  }

  object PrepareDataFromS3{
    def apply(): PrepareDataFromS3 = PrepareDataFromS3(bucket = "snowf0xrawdatacheck ")
  }
}
