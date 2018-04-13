Instructions to exceute the code.

1.	Create a new JAVA project in Eclipse IDE
2.	Add the relevant hadoop libraries.
3.	create a new package to create JAVA files. (say com.uncc.cloudProject)
4.	Download the files from the submitted code. Under the package copy the following JAVA files.
		DataProcess.java
		Pearson_Calculation.java
		Prediction.java
		Recommendation.java
		Similarity_Calculation.java
5.	First, execute the Similarity_Calculation.java or Pearson_Calculation.java file.
		Arguments to be provided are: "/home/cloudera/CloudProject/test_0.txt" "/home/cloudera/CloudProject/similarity"
6.	Second, execute the Prediction.java file
		Arguments to be provided are: "/home/cloudera/CloudProject/test_0.txt" "/home/cloudera/CloudProject/similarity/part-r-00000" "/home/cloudera/CloudProject/test_0.txt" "/home/cloudera/CloudProject/accuracyOut"
7.	Thrid, execute the Recommendation.java file
		Arguments to be provided are: "/home/cloudera/CloudProject/test_0.txt" "/home/cloudera/CloudProject/similarity/part-r-00000" "/home/cloudera/CloudProject/recommendList"
8.	Outputs are stored as:
		Similarity List Output: /home/cloudera/CloudProject/similarity
		Prediction Output: This is stored in intermediate path. /home/cloudera/workspace/cloud_version1/intPath/intermediate1
		Accuracy Output: /home/cloudera/CloudProject/accuracyOut
		Recommendation Output: /home/cloudera/CloudProject/recommendList


Instructions to execute the code on Hadoop Cluster.


1.	Open Terminal in Cloudera and connect it with NINERNET credentials.
2.	Downlaod the submitted files.
3.	Open terminal and copy all the files from the local machine to the cluster
		scp -v -r <path where the files are kept in local machine> aamula@dsba-hadoop.uncc.edu:~/<path of the cluster>
4.	Copy all the input files in hadoop folder
		hadoop fs -put <path where the files are kept in cluster> <input path in hdfs>
5.	Execute the following command.
		>>	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA fileName> -d build -Xlint
		>>	jar -cvf <filename>.jar -C build/ .
		>>	hadoop jar <filename>.jar org.myorg.<java filename> <input Files path> <output Files path>
6.	Output Files can be copied from cluster to local machine
