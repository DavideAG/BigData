/* Exam 16072018 */
// Part I
// Q1	-> Answer: B
// Q2	-> Answer: D

// Part 2

/* Input files:
 *
 * 	Robots.txt
 * 		RID,PlantID,MAC
 *
 * 	Faults.txt
 * 		RID,FaultTypeCode,FaultDuration,Date,Time
 *
 */

// MapReduce

protected void map(LongWritable key,
		Text value,
		Context context) {
	String[] fields = value.toString().split(",");
	String RID = fields[0];
	String faultCode = fields[1];
	String date = fields[3];

	if (faultCode.equals("FCode100")) {
		if (date.startsWith("2015/01")) {
			context.write(new Text(RID), new IntWritable(1));
		}

		if (date.startsWith("2015/02")) {
			context.write(new Text(RID), new IntWritable(2));	
		}

	}
}

protected void reduce(Text key,
		Iterable<IntWritable> values,
		Context context) {
	bool jun = false;
	bool feb = false;
	int month = 0;

	for (IntWritable val : values) {
		month = val.get();
		
		if ((month == 1) && (jun == false)) {
			jun = true;
		}

		if ((month == 2) && (feb == false)) {
			feb = true;
		}

		if ((jun == true) && (feb == true)) {
			break;
		}
	}

	if ((jun == true) && (feb && true)) {
		context.write(new Text(key.toString(), NullWritable.get()));
	}
}


// Spark
/* Input files:
 *
 * 	Robots.txt
 * 		RID,PlantID,MAC
 *
 * 	Faults.txt
 * 		RID,FaultTypeCode,FaultDuration,Date,Time
 *
 */

// textFile		-> Faults.txt
// filter		-> date = 2015/01 || 2015/02 || 2015/03 ||		CACHE
// 				  2015/04 || 2015/05 || 2015/06
// mapToPair		-> RID, 1
// reduceByKey		-> RID, totalNumberOfFails
// textFile		-> Robots.txt						
// mapToPair		-> RID,PlantID
// join			-> RID, Tuple2(totalNumberOfFails, PlantID)
// mapToPair		-> PlantID, totalNumberOfFails
// reduceByKey		-> PlantID, numberOfFailsPerPlant
// filter		-> numberOfFailsPerPlant > 180
// saveAsTextFile


// faulting robot:
// 	*at least 5 faults in each of the first 6 months of the year 2015
// 	*at least one fault of that robot in the first semester of the year 2015
// 	 has a FaultDuration greater than 120 minutes

// form filtered faults
// mapToPair		-> RID_month, (1, duration)
// reduceByKey		-> RID_month, (totFaults, maxDuration)
// filter		-> totFaults >= 5
// mapToPair		-> RID, (partialMaxFaultDuration, 1)
// 			   (1 is the total number of occurrences that must be equal to 6. Like time windows. We have to count the
// 			    total number of month, so I can remove month from the key value and add a counter to the value in
// 			    order to use it in the next reduceByKey function)
// reduceByKey		-> RID, (totalMaxFaultDuration, totalNumberOfOccurrences)
// filter		-> totalNumberOfOccurrences == 6 and totalMaxFaultDuration > 120
// keys().saveAsTextFile()
