package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPrices = args[0];
		Integer valueOfNW = Integer.parseInt(args[1]);
		String outputFolderA = args[2];
		String outputFolderB = args[3];

		SparkConf conf = new SparkConf().setAppName("examSpark - exe 2");

		JavaSparkContext sc = new JavaSparkContext(conf);


		/* part A */

		JavaRDD<String> pricesInput = sc.textFile(inputPrices);

		JavaRDD<String> only2016 = pricesInput.filter((String line) -> {
			String[] fields = line.split(",");

			String year = fields[1].split("/")[0];

			if (year.compareTo("2016") == 0)
			{
				return true;
			} else
			{
				return false;
			}
		});

		JavaPairRDD<String, Double> pairStocksRDD = only2016.
		mapToPair((String line) -> {
			String[] fields = line.split(",");

			Double price = Double.parseDouble(fields[3]);

			return new Tuple2<String, Double>
			(fields[0] + "_" + fields[1], price);
		}).cache();

		JavaPairRDD<String, Double> pairStockReducedRDD = pairStocksRDD.
		reduceByKey((Double a, Double b) -> {
			return a.compareTo(b) < 0 ? a : b;
		});

		pairStockReducedRDD.sortByKey().saveAsTextFile(outputFolderA);


		/* part B */

		// taking only the highest value of the same stockId_date
		JavaPairRDD<String, Double> highestPriceForEachDate = pairStocksRDD.
		reduceByKey((Double a, Double b) -> {
			return a.compareTo(b) > 0 ? a : b;
		});

		// now i'm focusing on stockId_week because I have to compute the new
		// result for the following week inside the value, and object of
		// FirstDatePriceLastDatePrice type has been attached in order to store
		// information about the price and the associated date necessary to
		// determiante if the analyzed week is positive or not
		JavaPairRDD<String, FirstDatePriceLastDatePrice> firstStepRDD = 
		highestPriceForEachDate.mapToPair((Tuple2<String, Double> entry) -> {
			String[] fields = entry._1().split(",");
			String stockId = fields[0];
			String date = fields[1];
			Double price = entry._2();

			Integer weekNum = DateTool.weekNumber(date);
			FirstDatePriceLastDatePrice dobj =
			new FirstDatePriceLastDatePrice(date, price, date, price);

			return new Tuple2<String, FirstDatePriceLastDatePrice>
			(stockId + "_" + weekNum, dobj);
		});

		// getting only one FirstDatePriceLastDatePrice object that represents
		// the final object of the considered week in the end we can use the
		// highest price of the stock associated with the first and the last
		// day of the week
		JavaPairRDD<String, FirstDatePriceLastDatePrice> weeksReadyRDD =
		firstStepRDD.reduceByKey((a, b) -> {
			String firstDate, lastDate;
			Double firstPrice, lastPrice;

			if (a.firstdate.compareTo(b.firstdate) < 0)
			{
				firstDate = a.firstdate;
				firstPrice = a.firstprice;
			} else {
				firstDate = b.firstdate;
				firstPrice = b.firstprice;
			}

			if (a.lastdate.compareTo(b.lastdate) > 0)
			{
				lastDate = a.lastdate;
				lastPrice = a.lastprice;
			} else {
				lastDate = b.lastdate;
				lastPrice = b.lastprice;
			}

			return new FirstDatePriceLastDatePrice
			(firstDate, firstPrice, lastDate, lastPrice);
		});

		// holding only positive weeks
		JavaPairRDD<String, FirstDatePriceLastDatePrice> posiveWeeksRDD =
		weeksReadyRDD.
		filter((Tuple2<String, FirstDatePriceLastDatePrice> entry) -> {
			return entry._2().lastprice.compareTo(entry._2().firstprice) > 0 ?
			true : false;
		});

		// now we have to count, for each stockId, how many positive weeks are
		// present
		JavaPairRDD<String, Integer> counterPositiveWeeksRDD = posiveWeeksRDD.
		mapToPair((Tuple2<String, FirstDatePriceLastDatePrice> entry) -> {
			String[] fields = entry._1().split("_");

			String stockId = fields[0];
			return new Tuple2<String, Integer>(stockId, new Integer(1));
		});

		// sum all occurrences in order to count how many positive weeks we have
		// for the same stockId
		JavaPairRDD<String, Integer> totalPositiveWeeksPerStockId =
		counterPositiveWeeksRDD.reduceByKey((a, b) -> {
			return a+b;
		});

		// getting only stockId where total positive week > NW
		JavaPairRDD<String, Integer> resultStocksIdRDD =
		totalPositiveWeeksPerStockId.filter((Tuple2<String, Integer> entry) -> {
			return entry._2().compareTo(valueOfNW) > 0? true : false;
		});

		resultStocksIdRDD.keys().saveAsTextFile(outputFolderB);

		sc.close();
	}
}
