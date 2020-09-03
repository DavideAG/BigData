/*
 * 20190718 version A
 *
 * 	Part 1
 */

/* 
 * Q1	-> 126/128 + 128/128 + 258/128 = 1 + 1 + 3 = 5	-> b 
 * log2016	126MB
 * log2017	128MB
 * log2018	258MB
 * 
 * block size	128MB
 */

/*
 * Q2	-> b (number of reducers = 10 but the reduce method is invoked
 * 	      3 times because there are three different dates)
 */


/*
 * Part II
 */

/*
 * Input files:
 *
 * 	Patches.txt
 * 		PID,Date,ApplicationName,BriefDescription
 *
 */

// MapReduce application
// Sw application with an increasing number of patches in the two years 2017-2018

protected void map(LongWritable key,
		Text value,
		Context context)
{
	String[] fields = value.toString().split(",");
	String date = fields[1];
	String AppName = fields[2];

	// 1 means 2017
	// 0 means 2018
	if (date.startsWith("2017") == true)
	{
		context.write(new Text(AppName), new IntWritable(1));
	}

	if (date.startsWith("2018") == true)
	{
		context.write(new Text(AppName), new IntWritable(0));
	}
}

protected void reduce(Text key,
		Iterable<IntWritable> values,
		Context context)
{
	int occurr17 = 0;
	int occurr18 = 0;
	int flag = 0;

	for (IntWritable value : values)
	{
		flag = value.get();
		
		if (flag == 1)
			occurr17 += 1;
		if (flag == 0)
			occurr18 += 1;
	}

	if (occurr18 > occurr17)
		context.write(new Text(key.toString()), NullWritable.get());
}

/*
 * Input files:
 *
 * 	Patches.txt
 * 		PID,Date,ApplicationName,BriefDescription
 *
 */
// Exercise 2 - Spark and RDDs

// Comparison between Windows10 and Ubuntu 18.04 during year 2017
// 1. high level schema
/*
 * textFile(Patches.txt)
 * filter	-> year = 2017 and application = "Windows 10" or "Ubuntu 18.04"
 * mapToPair	-> month, (occurrWin [0/1], occurrUbuntu [0/1])
 * reduceByKey	-> month, (totOccurrWin, totOccurrUbuntu)
 * filter	-> discard totOccurrWin = totOccurrUbuntu
 * mapValues	-> if win > ubu -> "W" else "U"
 * saveAsTextFile
 */

public static void main(String[] args) {

	String inputPatches;
	String outputPathPartA;
	String outputPathPartB;

	inputPatches = args[0];
	outputPathPartA = args[1];
	outputPathPartB = args[2];

	// Create a configuration object and set the name of the application
	SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_18 - Exercise #2");

	// Create a Spark Context object
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	// Reading form the input file
	JavaRDD<String> patchesRDD = sc.textFile(inputPatches).cache();

	JavaRDD<String> filteredRDD = patchesRDD.filter(line -> {
		String[] fields = line.split(",");
		String date = fields[1];
		String year = date.split("/")[0];
		String month = date.split("/")[1];
		String appName = fields[2];

		if (year.equals("2017") &&
				(appName.equals("Windows 10") ||
				appName.equals("Ubuntu 18.04"))
			return true;
		else
			return false;
	});

	JavaPairRDD<String, ctrWinUbu> firstOccurr = filteredRDD.mapToPair(line -> {
		String[] fields = line.split(",");
		String appName = fields[2];
		String date = fields[1];
		String year = date.split("/")[0];
		String month = date.split("/")[1];
		ctrWinUbu myCtr;
			
		if(appName.equals("Windows 10"))
			myCtr = new ctrWinUbu(1, 0);
		else
			myCtr = new ctrWinUbu(0, 1);

		return new Tuple2<String, ctrWinUbu>(month, myCtr);
	});

	JavaPairRDD<String, ctrWinUbu> secondOccurr = firstOccurr.reduceByKey((c1, c2) -> {
		return new ctrWinUbu(
				c1.occurrWin + c2.occurrWin,
				c1.occurrUbu + c2.occurrUbu);
	});

	JavaPairRDD<String, ctrWinUbu> onlyDifferentOccurrRDD = secondOccurr.filter(entry -> {
		return entry._2().occurrWin != entry._2().occurrUbu;
	});

	JavaPairRDD<String, String> finalResRDD = onlyDifferentOccurrRDD.mapValues(ctr -> {
		return ctr.occurrWin > ctr.occurrUbu ? new String("W") : new String("U");
	});

	finalResRDD.saveAsTextFile(outputPathPartA);

	
	// Exercise 2
	/*
 	* Input files:
 	*
 	* 	Patches.txt
 	* 		PID,Date,ApplicationName,BriefDescription
 	*
 	*/
	// windows of three consecutive months with many patches in year 2018
	// select the windows of three consecutive months of year 2018 with at
	// least 4 patches.
	//
	// Output:
	// 	first month of the selected windows and the associte sw
	// 	application.
	// 	e.g., (1, "Acrobat")

	// Finally, print on the stdout the number of sw applications associates
	// with at least one of the selected windows

	/* High level schema */
	/*
	 * FROM Patches.txt
	 * filter		-> year = 2018
	 * mapToPair		-> AppName_month, 1
	 * reduceByKey		-> AppName_month, totPatches
	 * filter		-> totPatches >= 4
	 * flatMapToPair	-> AppName_month	, 1
	 * 			   AppName_month-1	, 1
	 * 			   AppName_month-2	, 1
	 * reduceByKey		-> AppName_month, totOccurrMonths
	 * filter		-> totOccurrMonths == 3
	 * mapToPair		-> month, AppName 
	 * save
	 *
	 * for the last point
	 * X.values().distinct().count()
	 */

	// example target information	-> (AppName, (month, numpatches))
	
	JavaRDD<String> only18 = patchesRDD.filter(line -> {
		String[] fields = line.split(",");
		String date = fields[1];

		return date.startsWith("2018");
	});

	JavaPairRDD<String, Integer> firstCtrRDD = only18.mapToPair(line -> {
		String[] fields = line.split(",");
		String month = fields[1].split("/")[1];
		String appName = fields[2];

		return new Tuple2<String, Integer>(appName + "_" + month, new Integer(1));
	});

	JavaPairRDD<String, Integer> secondCtrRDD = firstCtrRDD.reduceByKey((v1, v2) -> v1 + v2);

	JavaPairRDD<String, Integer> onlyOverThresholdRDD = secondCtrRDD.filter(pair -> {
		int totPatches = pair._2();
		return totPatches >= 4;
	});

	JavaPairRDD<String, Integer> remappedRDD = onlyOverThresholdRDD.flatMapToPair(Tuple2<String, Integer> entry -> {
		ArrayList<String, Integer> localArray = new ArrayList<String, Integer>();

		Strgin appName = entry._1().split("_")[0];
		Integer month = Integer.parseInt(entry._1().split("_")[2]);
		Integer yesterday = new Integer(month - 1);
		Integer twoDaysAgo = new Integer(yesterday -1);

		localArray.add(appName + "_" + month, new Integer(1));
		localArray.add(appName + "_" + yesterday, new Integer(1));
		localArray.add(appName + "_" + twoDaysAgo, new Integer(1));

		return localArray.iterator();

	});

	JavaPairRDD<String, Integer> totalSumMonthsRDD = remappedRDD.reduceByKey((v1, v2) -> v1 + v2);

	JavaPairRDD<String, Integer> filteredTotalSumRDD = totalSumMonthsRDD.filter(entry -> entry._2() == 3);

	JavaPairRDD<String, String> remappedFilteredTotalSumRDD = filteredTotalSumRDD.mapToPair(pair -> {
		String[] fields = pair._1().split("_");
		String appName = fields[0];
		String month = fields[1];

		return new Tuple2<String, String>(month, appName);
	});

	remappedFilteredTotalSumRDD.saveAsTextFile(outputPathPartB);

	System.out.println("Number of software applications with at least one selected window = " +
			remappedFilteredTotalSumRDD.values().distinct().count());

	sc.close();

}
