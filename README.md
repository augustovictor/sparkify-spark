# sparkify-spark

This project is an ETL to process sparkify music stream logs generated by users while using the mobile app.

## Scenario

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## EMR Script Components
- `aws emr`: Invokes the AWS CLI, and specifically the command for EMR.
- `create-cluster`: Creates a cluster
- `--name`: You can give any name for this - this will show up on your AWS EMR UI. This can be duplicate as existing EMR.
- `--release-label`: This is the version of EMR you’d like to use.
- `--instance-count`: Annotates instance count. One is for the primary, and the rest are for the secondary. For example, if `--instance-count` is given 4, then 1 instance will be reserved for primary, then 3 will be reserved for secondary instances.
- `--application`: List of applications you want to pre-install on your EMR at the launch time
- `--bootstrap-action`: You can have a script stored in S3 that pre-installs or sets
environmental variables, and call that script at the time EMR launches
- `--ec2-attributes KeyName`: Specify your permission key name, for example, if it is `MyKey.pem`, just specify MyKey for this field. [Generate AWS Key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#prepare-key-pair).
- `--instance-type`: Specify the type of instances you want to use. Detailed list can be accessed here, but find the one that can fit your data and your budget.
- `--log-uri`: S3 location to store your EMR logs in. This log can store EMR metrics and also the metrics/logs for submission of your code.

### TODO
- Execute spark queries to answer business questions using the approach schema-on-read;
- Consider creating views for further processing or analytics;
- Discuss the difference of processing data on batch and streaming;
- Use `--auto-terminate` to make processing a one-shot processing;

#### Steps (Jupyter Notebook)
1. Create a cluster with emr version <= 5.29
	- Remember a cluster should have spark, hadoop, YARN, zeppelin;
	- [Aws says  Hadoop, Spark, and Livy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-working-with.html)
1. The aws service role should be `EMR_Notebooks_DefaultRole` or `create default role` if it does not exist yet;

#### Steps (SSH)
1. Generate key pair and set file permission: `chmod 400 <KEY_NAME>.pem`;
1. Create InstanceProfile EMR_EC2_DefaultRole with policy (AmazonElasticMapReduceforEC2Role)
1. Create service role EMR_DefaultRole with policy AmazonElasticMapReduceRole
1. Execute EMR creation script
1. Change emr SG so the cluster it accepts ssh requests (ElasticMapReduce-master port 22)
	- `ssh -i ~/<KEY_NAME>.pem hadoop@<EC2_MASTER_PUBLIC_IP.compute-1.amazonaws.com`
1. Change emr ec2 SG so you can login ec2 instances (ElasticMapReduce-slave)
1. Configure [FoxyProxy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html) to be able to view websites hosted on master node (jupyter notebook or sparkUi)
1. Upload data to s3
1. Upload script to master node home dir
	- `scp -i <KEY_NAME>.pem teste.txt hadoop@<EC2_PUBLIC_IP>:/home/hadoop`
1. Download data to spark's master node
1. Execute script with `spark-submit --master yarn <script>.py`
	- Possible to locate `spark-submit` with `which spark-submit`;
	- have `spark.stop` at the end of the script
1. Check spark ui to analyze execution
1. Save results back to s3

## References
- [Udf's return types](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-types.html)
- [EMR pricing](https://aws.amazon.com/emr/pricing/)
- [FoxyProxy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html)
- [Enhanced s3 protocol while loading from/to s3](https://sparkbyexamples.com/spark/write-read-csv-file-from-s3-into-dataframe/)
- (Spark aggregations)(https://mungingdata.com/apache-spark/aggregations/)
- [Pyspark sql types](https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/sql/types.html)
- [What are the things to consider before creating a Spark cluster for production?](https://www.quora.comWhat-are-the-things-to-consider-before-creating-a-Spark-cluster-for-production)
- [Apache Spark – Best Practices](https://www.bi4all.pt/en/news/en-blog/apache-spark-best-practices)
- [Tips and Best Practices to Take Advantage of Spark 2.x](https://mapr.com/blog/tips-and-best-practices-to-take-advantage-of-spark-2-x)
- [Best practices for running Apache Spark applications using Amazon EC2 Spot Instances with Amazon EMR](https://aws.amazon.com/blogs/big-data/best-practices-for-running-apache-spark-applications-using-amazon-ec2-spot-instances-with-amazon-emr)
- [Set up Apache Spark on a Multi-Node Cluster](https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b)
- [Spark performance tuning from the trenches](https://medium.com/teads-engineering/spark-performance-tuning-from-the-trenches-7cbde521cf60)
- [Best practices for tuning Spark applications](https://content-dsxlocal.mybluemix.net/docs/content/SSAS34_current/local-dev/best-practices-tuning_spark_application.html)
- [Apache Spark Cluster Setup](https://www.tutorialkart.com/apache-spark/how-to-setup-an-apache-spark-cluster)
- [Spark date and time functions](https://sparkbyexamples.com/spark/spark-sql-date-and-time-functions/)

## Gotchas

- Hi @NamitaP I was having the same issue, saving the data back to S3 was taking too long. Add this your main, spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2"), after you instantiate your Spark session. It will make it save much faster. You can read a bit more here: https://intellipaat.com/community/14967/extremely-slow-s3-write-times-from-emr-spark and https://kb.databricks.com/data/append-slow-with-spark-2.0.0.html