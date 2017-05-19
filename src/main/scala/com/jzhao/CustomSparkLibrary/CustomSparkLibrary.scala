package com.jzhao.CustomSparkLibrary

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import java.util.Calendar

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}

import scala.collection.JavaConversions.collectionAsScalaIterable

object CustomLibrary{

  case class MetaData(fileName:String, accessTimeStamp: Timestamp){}

  case class FilePackage(dataDF: DataFrame, meta: MetaData){
    def showZeppelinChart() = Util.zeppelinVisualize(dataDF)
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
    def getFileAsDF(fileName: String): DataFrame
    def listFiles(prefix: String): List[String]
    def getFileAsPackage(fileName:String): FilePackage
  }

  case class PrepareDataFromS3(bucket: String) extends PrepareData[String, PrepareDataFromS3] {

    override def getFileAsDF(fileName: String): DataFrame = Util.getS3file(bucket, fileName)

    override def listFiles(prefix: String): List[String] = Util.listS3files(s3Client, bucket, prefix)(s => (s.getKey))

    override def getFileAsPackage(fileName: String): FilePackage = FilePackage(Util.getS3file(bucket, fileName), MetaData(fileName))

    def setBucket(bucketName: String): PrepareDataFromS3 = copy(bucket = bucketName)
  }

  object PrepareDataFromS3{
    def apply(): PrepareDataFromS3 = PrepareDataFromS3(bucket = "snowf0xrawdatacheck ")
  }

  object Util{

    def listS3files[T](s3: AmazonS3, bucket: String, prefix: String)(f: (S3ObjectSummary) => T) = {
      def scan(acc: List[T], listing: ObjectListing): List[T] = {
        val summaries = collectionAsScalaIterable[S3ObjectSummary](listing.getObjectSummaries())
        val mapped = (for (summary <- summaries) yield f(summary)).toList

        if(!listing.isTruncated) mapped
        else scan(acc ::: mapped, s3.listNextBatchOfObjects(listing))
      }
      scan(List(),s3.listObjects(bucket, prefix))
    }

    def getS3file(bucket: String, fileName: String, header: Boolean = true, inferSchema: Boolean = true):DataFrame= spark.read.option("header", header).option("inferSchema", inferSchema).csv("s3n://"+bucket+"/"+fileName)

    def zeppelinVisualize(dataDF:DataFrame):String = {
      var chart = ("%table "+dataDF.columns.mkString("\t")+"\n")
      dataDF.collect().foreach(x=>chart = chart.concat(x.mkString("\t")+"\n"))
      chart
    }
  }
}
