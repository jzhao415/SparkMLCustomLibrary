# SparkMLCustomLibrary
This is a demo library for Spark ML purpose

The purpuse of this code is to demo:
1. retrieve a csv file from S3
2. create metadata along with data file,
3. transfer into DataFrame,
4. function to show visualization/chart in zeppelin 
5. combine with spark ml pipeline (TBD)

End user should able to do:

1. quick retrieve a csv file with file name, using default s3 bucket in code
```scala
val preparedData: DataFrame = PrepareDataFromS3("S3_file_name").toDF() 
```

2. retrieve a csv file from a specific bucket, using filename and bucket name
```scala
val preparedData: DataFrame = PrepareDataFromS3(fileName="S3_file_name", bucket="bucket_name").toDF() //use specific s3 bucket
```

3. retrieve a csv file from S3, apply new meta data
```scala
val prepareData:PrepareDataFromS3 = PrepareDataFromS3("S3_file_name).getAndApplyMeta("new meta data")
```

4. in zeppelin
```scala
PrepareDataFromS3("S3_file_name).zeppelinChart()
```
![alt text](https://github.com/snowf0x/SparkMLCustomLibrary/blob/master/resource/readme_image_chart1.PNG)
![alt text](https://github.com/snowf0x/SparkMLCustomLibrary/blob/master/resource/readme_image_chart2.PNG)
