# SparkMLCustomLibrary
This is a demo library for Spark ML purpose

The purpuse of this code is to demo:
1.retrieve a csv file from S3
2.create metadata along with data file,
3.transfer into DataFrame,
4.function to show visualization/chart in zeppelin 
5.combine with spark ml pipeline (TBD)

end user should able to do:

quick retrieve a csv file with file name, using default s3 bucket in code
```scala
val preparedData: DataFrame = PrepareDataFromS3("S3_file_name").toDF() 
```

retrieve a csv file from a specific bucket, using filename and bucket name
```scala
val preparedData: DataFrame = PrepareDataFromS3(fileName="S3_file_name", bucket="bucket_name").toDF() //use specific s3 bucket
```

retrieve a csv file from S3, apply new meta data
```scala
val prepareData:PrepareDataFromS3 = PrepareDataFromS3("S3_file_name).getAndApplyMeta("new meta data")
```

in zeppelin
```scala
PrepareDataFromS3("S3_file_name).zeppelinChart()
```