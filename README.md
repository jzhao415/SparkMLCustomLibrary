# SparkMLCustomLibrary
This is a demo library for Spark ML purpose

The purpuse of this code is to demo:
retrieve a csv file from S3, 
create metadata along with data file,
transfer into DataFrame,
function to show visualization/chart in zeppelin 
combine with spark ml pipeline (TBD)

end user should able to do:
//quick retrieve a csv file with file name, using default s3 bucket in code
val preparedData: DataFrame = PrepareDataFromS3("S3_file_name").toDF() 

//retrieve a csv file from a specific bucket, using filename and bucket name
val preparedData: DataFrame = PrepareDataFromS3(fileName="S3_file_name", bucket="bucket_name").toDF() //use specific s3 bucket

//retrieve a csv file from S3, apply new meta data
val prepareData:PrepareDataFromS3 = PrepareDataFromS3("S3_file_name).getAndApplyMeta("new meta data")

//in zeppelin
PrepareDataFromS3("S3_file_name).zeppelinChart()
