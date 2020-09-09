/* Lecture 24bis */

/* Input file
 * 	Patches.txt
 *		PID,Date,ApplicationName,BriefDescription
 */

// MapReduce and Hadoop exercise
// Software applications with an increasing number of patches
// in the two years 2017 - 2018


protected void map(LongWritable key,
		Text value,
		Context context) {

	String[] fields = value.toString().split(",");
	String date = fields[1];
	String AppName = fields[2];

	if (date.startsWith("2017") {
		context.write(new Text(AppName), new IntWrtiable(7));
	} else if (date.startsWith("2018")) {
		context.write(new Text(AppName), new IntWrtiable(8));

	}
}

protected void reduce(Text key,
	Iterable<IntWritable> values,
	Context context) {
	
	int identifier = 0;
	int numOccurr7 = 0;
	int numOccurr8 = 0;

	for (IntWritable val : values) {
		identifier = val.get();

		if (identifier == 7) {
			numOccurr7 += 1;
		} else {
			numOccurr8 += 1;
		}
	}

	if (numOccurr8 > numOccurr7)
		context.write(new Text(key.toString()), NullWritable.get());
}


// Spark
/* Input file
 * 	Patches.txt
 *		PID,Date,ApplicationName,BriefDescription
 */

//	high level schema
// textFile		-> Patches.txt
// filter		-> year = 2017 && ApplicationName = Windows 10 or Ubuntu 18.04
// mapToPair		-> month, (patchWin10 [0/1], patchUbuntu[0/1])
// reduceByKey		-> month, (totPatchWin10, totPatchUbuntu)A
// filter		-> totPatchWin10 != totPatchUbuntu
// mapValues		-> if totPatchWin10 > totPatchUbuntu -> "W"
// 			   else -> "U"
// saveAsTextFile

//	high level schema
// textFile		-> Patches.txt
// filter		-> year = 2018
// mapToPair		-> month_software, 1
// reduceByKey		-> month_software, totNumPatches
// filter		-> totNumPatches > 4
// flatMapToPair	-> month_software, 1
// 			   month-1_software, 1
// 			   month-2, software, 1
// reduceByKey		-> sum all values for each month_software
// filter		-> numOccurrences == 3
// mapTopair		-> month, softwareName		CACHE
// saveAsTextFile
// print		-> values().distinct().count()
