# SparkMLCustomLibrary
This is a demo library for Spark ML related project

The purpose of this library is to demostrate:
1. retrieve a csv file from S3
2. create metadata along with data file,
3. transfer into DataFrame,
4. function to show visualization/chart in zeppelin 
5. combine with spark ml pipeline (TBD)

Compete Test cases are under /src/test

Build Instruction:
```
mvn clean install
```

Usage: 

1. quick retrieve a csv file with file name, using default s3 bucket in code
```scala
val preparedData: DataFrame = PrepareDataFromS3().getFileAsDF("table.csv")
```

2. retrieve a csv file from a specific bucket, using filename and bucket name
```scala
val preparedData: DataFrame = PrepareDataFromS3().setBucket("snowf0xrawdata").getFileAsDF("table.csv")
```

3. retrieve a csv file from S3, apply new meta data
```scala
val filePackage: FilePackage = PrepareDataFromS3().setBucket("snowf0xrawdata").getFileAsPackage("table.csv")
```

4. in zeppelin
```scala
val filePackage:FilePackage =PrepareDataFromS3().setBucket("snowf0xrawdata").getFileAsPackage("table.csv")
filePackage.showZeppelinChart()
```
![alt text](https://github.com/snowf0x/SparkMLCustomLibrary/blob/master/resource/readme_image_chart1.PNG)
![alt text](https://github.com/snowf0x/SparkMLCustomLibrary/blob/master/resource/readme_image_chart2.PNG)

5. in DataBricks
![alt text](https://github.com/snowf0x/SparkMLCustomLibrary/blob/master/resource/readme_image_databricks_demo.PNG)