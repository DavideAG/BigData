/* Exam 03092018 - Version I */
//Q1 -> Answer: D
//Q2 -> Answer: C

/* Input file:
 * 	Stocks_Prices.txt
 * 		stockId,date,hour:minute,price
 */

protected void map(LongWritable key,
		Text value,
		Context context) {
	String[] fields = value.toString().split(",");

	String stockId = fields[0];
	String date = fields[1];
	String hour_minute = fields[2];
	Double price = Double.parseDouble(fields[3]);

	if (date.startsWith("2017") && stockId.equals("GOOG")) {
		context.write(NullWritable.get(), new Text(price + "_" + date + "," + hour_minute));
	}
}

protected void reduce(NullWritable key,
		Iterable<Text> values,
		Context context) {
	
	Double highestPrice = Double.MIN_VALUE;
	Double actualPrice;
	String timeStmp = "";
	String tmp;

	for (Text val : values) {
		tmp = val.toString();
		actualPrice = Double.parseDouble(tmp.split("_")[0]);
		
		if (highestPrice < actualPrice) {
			highestPrice = actualPrice;
			timeStmp = tmp.split("_")[1];
		} else if(highestPrice.compareTo(actualPrice) == 0 &&
			  timeStmp.compareTo(tmp.split("_")[1] < 0)) {
			timeStmp = tmp.split("_")[1];
		}
	}

	context.write(new DoubleWritable(highestPrice), new Text(timeStmp));
}


/* Exercise 2 - part I */
// textFile		-> Stocks_Prices.txt
// filter		-> year = 2017		CACHE
// mapToPair		-> stockId_date, (currPrice, currPrice)
// reduceByKey		-> stockId_date, (highestPrice, lowestPrice)	CACHE
// filter		-> highestPrice - lowestPrice > 10
// mapToPair		-> stockId_date, 1
// reduceByKey		-> stockId_date, totalNumberOfVariations
// saveAsTextFile

/* Exercise 2 - partII  */
// from 	stockId_date, (highestPrice, lowestPrice)
// mapToPair		-> stockId_date, dailyVariation
// flatMapToPair	-> stockId_date, (dailyVariation, 1)
// 			   stockId_date-1, (dailyVariation, 1)
// reduceByKey		-> abs(dailyVariation_1 - dailyVariation_2) AND sum the second element
// filter		-> second_element == 2 AND first_element <= 0.1
// mapToPair		-> stockId, date
// saveAsTextFile
