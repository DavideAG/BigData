/* Exam 18-07-2019 Version B */

//	Part I
/*
 * Question 1	Answer: C
 *
 * log2016	1022MB
 * log2017	1024MB
 * log2018	2050MB
 *
 * BS		1024MB
 *
 * #mappers	1 + 1 + 3 = 5
 */

/*
 * Question 2	Answer: B 
 *
 * 20 instances of the reducer but it
 * is invoked four times.
 */

//	Part II

/* Input files:
 *
 * 	Commits.txt
 * 		CID,Timestamp,Project,ContributorNickname,Description
 */

//	MapReduce application
// Contributors with an increasing number of commits in the two months
// May 2019-June 2019
protected void map(LongWritable key,
		Text value,
		Context context) {
	String[] fields = value.toString().split(",");

	String contributorNickname = fields[3];
	String date_hour = fields[1];

	// 1 -> May
	// 0 -> June
	if (date_hour.startsWith("2019/05"))
	{
		context.write(new Text(contributorNickname), new IntWritable(1));
	}

	if (date_hour.startsWith("2019/06"))
	{
		context.write(new Text(contributorNickname), new IntWritable(0));
	}
}

protected void reduce(Text key,
		Iterable<IntWritable> values,
		Context context) {

	int numMayCommits = 0;
	int numJunCommits = 0;

	for (IntWritable val : values)
	{
		int flagMonth = val.get();

		if (flagMonth == 1)
			numMayCommits += 1;
		else
			numJunCommits += 1;
	}

	if (numJunCommits > numMayCommits)
		context.write(new Text(key.toString()), NullWritable.get());

}

//	Part 2

// Point A 

/* Input files:
 *
 * 	Commits.txt
 * 		CID,Timestamp,Project,ContributorNickname,Description
 */

/* For each date of month June 2019 select the project with more commits
 * between "Apache Spark" and "Apache Flink". If the number of commits
 * is the same, no output will be produces for those entries.
 *
 * Example of output:
 * 	(2019/06/01, "AF")
 * 	(2019/06/03, "AS")
 */

//	High level schema
// textFile	-> Commits.txt
// filter	-> june 2019
// mapToPair	-> date, (commitSpark [0/1], commitFlink [0/1])
// reduceByKey	-> date, (totCommitSpark, totCommitFlink)
// filter	-> totCommitSpark != totCommitFlink
// mapValues	-> if totCommitSpark > totCommitFlink -> "AS" else "AF"
// saveAsTextFile

public static void main(String[] args) {

	String inputCommits;
	String outputPathPartA;
	String outputPathPartB;

	inputCommits = args[0];
	outputPathPartA = args[1];
	outputPathPartB = args[2];

	// Create a configuration object and set the name of the application
	SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_18 - Exercise #2 v2");

	// Create a Spark Context object
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> commitsRDD = sc.textFile(inputCommits).cache();

	JavaRDD<String> jun19Commits = commitsRDD.filter(line -> {
		String[] fields = line.split(",");
		String date = fields[1];
		String project = fields[2];

		if (date.startsWith("2019/06") &&
		   (project.equals("Apache Spark") ||
		    project.equals("Apache Flink")))
			return true;
		else
		       return false;	

	});

	JavaPairRDD<String, commSpaCommFli> firstCtr = jun19Commits.mapToPair(line -> {
		String[] fields = line.split(",");
		String date = fields[1].split("_")[0];
		String project = fields[2];
		commSpaCommFli commCtr;

		if (project.equals("Apache Spark"))
			commCtr = new commSpaCommFli(1, 0);

		if (project.equals("Apache Flink"))
			commCtr = new commSpaCommFli(0, 1);

		return new Tuple2<String, commSpaCommFli>(date, commCtr);

	});

	JavaPairRDD<String, commSpaCommFli> secondCtr = firstCtr.reduceByKey((c1, c2) -> {
		return new commSpaCommFli(c1.commSpa + c2.commSpa,
					  c1.commFli + c2.commFli);
	});

	JavaPairRDD<String, commSpaCommFli> onlyDifferentRDD = secondCtr.filter(pair -> {
		return pair._2().commSpa != pair._2().commFli;
	});

	JavaPairRDD<String, String> mappedValues = onlyDifferentRDD.mapValues(cscf -> {
		if (cscf.commSpa > cscf.commFli)
			return new String("AS");
		else
			return new String("AF");
	});

	mappedValues.saveAsTextFile(outputPathPartA);


	/* Part B */
	/* for each project, select three consecutive windows of three months with many
	 * commits in 2017. At least 20 commits in each month of the selected window.
	 *
	 * Print on the standard output the number of project associated with at least
	 * one selected window.
	 */
	
	/* Input files:
 	*
 	* 	Commits.txt
 	* 		CID,Timestamp,Project,ContributorNickname,Description
 	*/


	//	High level schema
	// FROM commitsRDD
	// filter		-> year = 2017
	// mapToPair		-> projectName_month, 1
	// reduceByKey		-> projectName_month, totCommits
	// filter		-> totCommits >= 20
	// flatMapToPair	-> projectName_month, 1
	// 			   projectName_previour, 1
	// 			   projectName_2previous, 1
	// reduceByKey		-> projectName_month, totOccurrences
	// filter		-> totOccurrences == 3
	// mapToPair		-> month, projectName
	// saveAsTextFile
	//
	// .values().distinct().count()

	JavaRDD<String> filteredYear17 = commitsRDD.filter(line -> {
		String[] fields = line.split(",");
		String date = fields[1];
		return date.startsWith("2017");
	});

	JavaPairRDD<String, Integer> firstCountPairRDD = ilteredYear17.mapToPair(line -> {
		String[] fields = line.split(",");
		String month = fields[1].split("_")[0].split("/")[1];
		String projectName = fields[2];

		return new Tuple2<String, Integer>(projectName + "_" + month, new Integer(1));

	});

	JavaPairRDD<String, Integer> secondCountPairRDD = firstCountPairRDD.
		reduceByKey((v1, v2) -> v1 + v2);

	JavaPairRDD<String, Integer> manyCommits = secondCountPairRDD.
		filter(pair -> pair._2() >= 20);

	JavaPairRDD<String, Integer> remappedRDD = manyCommits.flatMapToPair(pair -> {
		ArrayList<String, Integer> local = new ArrayList<String, Integer>();
		Integer month = Integer.parseInt(pair._1().split("_")[1]);
		String projectName = pair._1().split("_")[0];

		local.add(new String(projectName + "_" + month), new Integer(1));
		local.add(new String(projectName + "_" + (month-1)), new Integer(1));
		local.add(new String(projectName + "_" + (month-2)), new Integer(1));

		return local.iterator();
	});

	JavaPairRDD<String, Integer> occurrencesSummarizedRDD = remappedRDD.
		reduceByKey((v1, v2) -> v1 + v2);

	JavaPairRDD<String, Integer> onlyThreeRDD = occurrencesSummarizedRDD.filter(pair -> {
		return pair._2() == 3;
	});

	JavaPairRDD<Integer, String> finalVersionRDD = onlyThreeRDD.mapToPair(pair -> {
		String[] fields = pair._1().split("_");
		String projectName = fields[0];
		Integer month = Integer.parseInt(fields[1]);
		return new Tuple2<Integer, String>(month, projectName);
	}).cache();

	finalVersionRDD.saveAsTextFile();

	System.out.println("Number of project associated with at least one selected window = " +
			finalVersionRDD.values().distinct().count());

}
