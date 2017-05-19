package com.jzhao.CustomSparkLibrary

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions.collectionAsScalaIterable

/**
  * Created by jzhao on 5/19/2017.
  */
object LibraryUtil{

  def listS3files[T](s3: AmazonS3, bucket: String, prefix: String)(f: (S3ObjectSummary) => T) = {
    def scan(acc: List[T], listing: ObjectListing): List[T] = {
      val summaries = collectionAsScalaIterable[S3ObjectSummary](listing.getObjectSummaries())
      val mapped = (for (summary <- summaries) yield f(summary)).toList

      if(!listing.isTruncated) mapped
      else scan(acc ::: mapped, s3.listNextBatchOfObjects(listing))
    }
    scan(List(),s3.listObjects(bucket, prefix))
  }

  def getS3file(spark: SparkSession, bucket: String, fileName: String, header: Boolean = true, inferSchema: Boolean = true):DataFrame= spark.read.option("header", header).option("inferSchema", inferSchema).csv("s3n://"+bucket+"/"+fileName)

  def zeppelinVisualize(dataDF:DataFrame):String = {
    var chart = ("%table "+dataDF.columns.mkString("\t")+"\n")
    dataDF.collect().foreach(x=>chart = chart.concat(x.mkString("\t")+"\n"))
    chart
  }
}