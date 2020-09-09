/* Exam 26062018 v2 */
// Q1	-> A: B
// Q2	-> A: C

// part II
// Input
// 	ClimateData
// 		SID,Timestamp,Precipitation_mm,WindSpeed

// 1
// textFile		-> ClimateData
// filter		-> 2018/04 || 2018/05		CACHE
// mapToPair		-> SID-hour, (precipitation_mm, WindSpeed, 1)
// 			   (I can accorpate the numberOfOccurrences!!!)
// reduceByKey		-> SID-hour, (totSumPrecipitation_mm, totSumWindSpeed, totOccurr)
// filter		-> totSumPrecipitation_mm / totOccurrP < thrP
// 			   totSumWIndSPeed / totOccurrW < thrW
// keys().saveAsTextFile()


// given a date and a sensor, a sensor measured an unbalanced wind speed if:
// 	* at least 5 hour for which the MINIMUM value of wind speed is less than 1 km/h
// 	* at least 5 hour for which the MINIMUM value of WindSpeed is greather than 40 km/h

// devo contare il massimo e il minimo di wind speed all'interno di ogni ora
// GOAL		sid-date_hour, (maxWindSpeed, MinWindSpeed)

// from cached (filtered april and may 2018)
// mapToPair		-> sid-date_hour, (currWIndSpeed)
// reduceByKey		-> sid-date_hour, minWindSpeedInOneHour
// filter		-> minWindSpeedInOneHour > 40 || minWindSpeedInOneHour < 1
// mapToPair		-> sid-date, (isMax[0/1], isMin[0/1])
// reduceByKey		-> sum all isMax and isMin
// filter		-> isMax > 5 && isMin > 5
// keys().saveAsTextFile()


public static void main(String[] args) {

        double precThr;
        double windThr;
        String inputPathPrices;
        String outputPathPartA;
        String outputPathPartB;

        precThr = Double.parseDouble(args[0]);
        windThr = Double.parseDouble(args[1]);
        inputPathPrices = args[2];
        outputPathPartA = args[3];
        outputPathPartB = args[4];

        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_06_26 - Exercise #2 - v2");

        // Create a Spark Context object
	JavaSparkContext sc = new JavaSparkContext(conf);

	/* Part I */
	JavaRDD<String> climateData = sc.textFile(inputPathPrices);

	JavaRDD<String> filteredMonths = climateData.filter(line -> {
		String[] fields = line.split(",");
		String timestamp = fields[1];

		if (timestamp.startsWith("2018/04") || timestamp.startsWith("2018/05"))
			return true;
		else
			return false;
	}).cache();

	JavaPairRDD<String, MyCounter> firstPair = filteredMonths.mapToPair(line -> {
		String[] fields = line.split(",");

		String sid = fields[0];
		String hour = fields[2].split(":")[0];
		Double precipitation = Double.parseDOuble(fields[3]);
		Double windSpeed = Double.parseDouble(fields[3]);

		return new Tuple2<String, MyCounter>(sid + "-" + hour, new MyCounter(precipitation, windSpeed, 1));
	});

	JavaPairRDD<String, MyCounter> secondPair = firstPair.reduceByKey((c1, c2) -> {
		return new MyCounter(c1.precipitation + c2.precipitation,
				     c1.windspeed + c2.windspeed,
				     c1.occurr + c2.occurr);
	});

	JavaPairRDD<String, MyCounter> filteredPair = secondPair.filter(pair -> {
		Double totSumPrecipitation_mm = pair._2().precipitation;
		Double totSumWindSpeed = pair._2().windSpeed;
		int occurrences = pair._2().occurr;
		
		if ((totSumPrecipitation_mm / occurrences < precThr) ||
		    (totSumWindSpeed / occurrences < windThr))
			return true;
		else
			return false;
	});

	filteredPair.keys().saveAsTextFile(outputPathPartA);



	/* Part II */
	JavaPairRDD<String, Double> windPair = filteredMonths.mapToPair(line -> {
		String[] fields = line.split(",");
		String sid = fields[0];
		String date = fields[1];
		String hour = fields[2].split(":")[0];
		Double windSpeed = Double.parseDouble(fields[4]);

		return new Tuple2<String, Double>(sid + "-" + date + "_" + hour, windSpeed);
	});

	JavaPairRDD<String, Double> windPairSum = windPair.reduceByKey((d1, d2) -> d1 + d2);

	JavaPairRDD<String, Double> windPairFiltered = windPairSum.filter(pair -> {
		Double windSpeed = pair._2();

		if ((windSpeed > 40) || windSpeed < 1)
			return true;
		else
			return false;
	});

	JavaPairRDD<String, OccurrCounter> occurrFirst = windPairFiltered.mapToPair(pair -> {
		String sidHour = pair._1().split("_")[0];
		OccurrCounter myCtr = new OccurrCounter(0, 0);
		Double windSpeed = pair._2();

		if ( windSpeed > 40 )
			myCtr.max = 1;
		if ( windSpeed < 1 )
			myCtr.min = 1;

		return new Tuple2<String, OccurrCounter>(sidHour, myCtr);
	});

	JavaPairRDD<String, OccurrCounter> occurrSecond = occurrFirst.reduceByKey((oc1, oc2) -> {
		OccurrCounter ocr = new OccurrCounter(oc1.max + oc2.max,
						      oc1.min + oc2.min);	
	});

	JavaPairRDD<String, OccurrCounter> filteredOccurr = occurrSecond.filter(pair -> {
		if ((pair._2().max > 5) && (pair._2().min > 5))
			return true;
		else
			return false;
	});

	filteredOccurr.saveAsTextFile(outputPathPartB);

}
