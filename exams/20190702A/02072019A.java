/* Exam 02-07-2019 */
	
//	Part I

/* Q1	-> Answer: A */

/* Q2	-> Answer: B
 * log2017	254MB
 * log2018	2050MB
 *
 * BS		256MB
 *
 * 1 + 9 = 10
 */

//	Part II

/* Input files:
 *
 * 	Bicycles.txt
 * 			BID,BicycleManufacturer,City,Country
 *
 * 	Bicycles_Failures.txt
 *	 		Timestamp,BID,Component
 */

// MapReduce application
// Italian cities with single manufacturer.
// Output: (city, manufacturer)

protected void map(LongWritable key,
		Text value,
		Context context) {
	
	String[] fields = value.toString().split(",");

	String city = fields[2];
	String country = fields[3];
	String manufacturer = fields[1];

	if (country.equals("Italy")) {
		context.write(new Text(city), new Text(manufacturer));
	}
}

protected void reduce(Text key,
		Iterable<Text> values,
		Context context) {
	
	bool different = 1;
	String firstManufacturier;


	for (Text val : values) {
		if (different == 1) {
			firstManufacturier = val.toString();
			different = 0;
			continue;
		}

		if (firstManufacturier.equals(val.toString()) == false) {
			different = 1;
			break;
		}
	}

	if (different != 1)
		context.write(new Text(key.toString()), new Text(firstManufacturier));
}


/* Input files:
 *
 * 	Bicycles.txt
 * 			BID,BicycleManufacturer,City,Country
 *
 * 	Bicycles_Failures.txt
 *	 		Timestamp,BID,Component
 */

// Spark application
public static void main(String[] args) {

	String inputBicycles;
	String inputFailures;
	String outputPathPartA;
	String outputPathPartB;

	inputBicycles = args[0];
	inputFailures = args[1];
	outputPathPartA = args[2];
	outputPathPartB = args[3];

	// Create a configuration object and set the name of the application
	SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_02 - Exercise #2");

	// Create a Spark Context object
	JavaSparkContext sc = new JavaSparkContext(conf);

	/* part A */

	/* Bycicles with more than two wheel failures in at least
	 * one month of year 2018. Store the BIDs of the selected bicycles.
	 */

	//	high level schema
	// textFile		-> Bicycles_Failures.txt
	// filter		-> year = 2018			CACHE
	// filter		-> component = well
	// mapToPair		-> BID_month, 1
	// reduceByKey		-> BID_month, totWellFailures
	// filter		-> totWellFailures > 2
	// map			-> BID
	// distinct
	// saveAsTextFile
	

	/* select cities where all the bicycles of that city are associated with
	 * at most (<=) 20 failures.
	 */
	//	high level schema
	// from year = 2018RDD
	// mapToPair		-> BID, 1
	// reduceByKey		-> BID, numTotFailues
	// 
	// from Bicycles.txt
	// mapToPair		-> BID, city
	//
	// join			-> BID, Tuple2<String, String>(city, numTotFailures)
	// values
	// reduceByKey		-> selecting the MAX numTotFailures
	// filter		-> numTotFailures <= 20		CACHE
	// saveAsTextFile
	// keys.count()
	//
	//
	// city, numberOfTotalFailures
