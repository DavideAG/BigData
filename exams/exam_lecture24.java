/* Lecture 24 | Exam 03/09/2018 */

/* Input file
 * 	Stock_Prices.txt
 * 	
 * 		stockId,date,hour:minute,price
 */

// MAPPER
protected void map(LongWritable key,
		Text value,
		Context context) {
	
	String[] fields = value.toString().split(",");

	String stockId = fields[0];
	String date = fields[1];
	String hour_minute = fields[2];
	String price = Double.parseDouble(fields[3]);
	
	if (date.startsWith("2017") && (stockId.equals("GOOG"))){
		context.write(new DoubleWritable(price), new Text(hour_minute));
	}
}


// REDUCER
Double price;
String hour_minute;

protected void setup(Context context) {
	price = Double.MIN_VALUE;
	hour_minute = null;	
}

protected void reduce(DoubleWritable key,
		Iterable<Text> values,
		Context context) {

	for (Text val : values) {

		if(price < key.get()) {
			price = key.get();
			hour_minute = val.toString();
		} else if (price == key.get() &&
		    	   hour_minute.compareTo(val.toString()) < 0) {
		 	hour_minute = val.toString();   
		 }
	
	}
}

protected void cleanup(Context context) {
	context.write(new DoubleWritable(price), new Text(hour_minute));
}


/* Spark exercise */
/* Input file
 * 	Stock_Prices.txt
 * 	
 * 		stockId,date,hour:minute,price
 */

// 2017, select for each stock and date the number of daily stock price
// variation greather than 10.

/* high level schema */
// textFile		-> Stock_prices
// filter		-> year = 2017
// mapToPair		-> stockId_date, (price, price)
// reduceByKey		-> stockId_date, (highest_price, lowest_price)	CACHE
// filter		-> highest_price - lowest_price > 10
// mapToPair		-> stockId, 1
// reduceByKey		-> stockId, numberOfTotalDays
// saveAsTextFile


/* stable trends in year 2017 for each stock.
 * daily price variation of the first date - daily price variarion of the second date < = 0.1
 *
 * Output:
 * 	stockId, first date of the stable thrend
 * 	stockId, first_date
 */
/* select all the sequences of two consecutive dates characterized by a "stable trend" */

/* high level schema */
// from cached files
// mapValues		-> stockId_date, dailyVariation	(dailyVariation = highest-lowest) */
// flatMapToPair	-> stockId_date, currVariation
// 			-> stockId_date-1, currVariation
// groupByKey		-> stockId_date, Iterable<variations>
// 	DEVO ACCERTARMI CHE CI SIANO ALMENO DUE GIORNI CONSECUTIVI!!!! 
// filter		-> stockId_date, abs(first-second) <= 0.1 and count if there
// 			   are 2 occurrences
// saveAsTextFile

