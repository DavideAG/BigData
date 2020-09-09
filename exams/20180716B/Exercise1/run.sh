# Remove folders of the previous run
hdfs dfs -rm -r exam_ex1_data
hdfs dfs -rm -r exam_ex1_out

# Put input data collection into hdfs
hdfs dfs -put exam_ex1_data


# Run application
hadoop jar target/Exam2018_07_16_Exercise1-1.0.0_v2.jar it.polito.bigdata.hadoop.exercise1.DriverBigData 2 exam_ex1_data/Faults.txt exam_ex1_out



