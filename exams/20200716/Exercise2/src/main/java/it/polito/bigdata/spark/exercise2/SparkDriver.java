package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	
	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		//String inputPathBooks;
		String inputPathPurchases;
		String outputPathPart1;
		String outputPathPart2;

		//inputPathBooks = "exam_ex2_data/Books.txt";
		inputPathPurchases = "exam_ex2_data/Purchases.txt";
		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").
		setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part 1
		// *****************************************

		JavaRDD<String> PurchasesRDD = sc.textFile(inputPathPurchases);

		JavaRDD<String> year2018RDD = PurchasesRDD.filter((String line) -> {
			String[] fields = line.split(",");

			String date = fields[2];

			if (date.startsWith("2018"))
				return true;
			else
				return false;
		});
		
		JavaPairRDD<String, Integer> mapCtrRDD = year2018RDD.mapToPair(
		(String line) -> {
			String[] fields = line.split(",");

			String date = fields[2];
			String BID = fields[1];

			return new Tuple2<String, Integer>(date + "_" + BID, new Integer(1));
		});

		// total number of purchases for each date_BID key
		JavaPairRDD<String, Integer> sumCtrRDD = mapCtrRDD.reduceByKey((v1, v2) -> {
			Integer sum = new Integer(v1 + v2);
			
			return sum;
		}).cache();

		JavaPairRDD<String, Integer> bipMaxRDD = sumCtrRDD.mapToPair((entry) -> {
			String[] fields = entry._1().split("_");

			String date = fields[1];
			String BID = fields[0];

			return new Tuple2<String, Integer>(BID, entry._2());
		}).cache();

		JavaPairRDD<String, Integer> reducedBipMaxRDD = bipMaxRDD.reduceByKey(
		(Integer v1, Integer v2) -> {
			if (v1.compareTo(v2) > 0) {
				return v1;
			} else {
				return v2;
			}
		});

		reducedBipMaxRDD.saveAsTextFile(outputPathPart1);



		/* part B */
		/**
		 * Vogliamo tutte le finestre temporali di 3 giorni consecutivi dove,
		 * per ogni libro, vi è stato un numero di acquisti superiore al 10%
		 * rispetto al numero di acquisti di quel libro nel 2018.
		 */

		// Computing the total number of purchases in 2018 for a certain book
		JavaPairRDD<String, Integer> totPurchasesForEachBidRDD = bipMaxRDD.
		reduceByKey((v1, v2) -> v1 + v2);

		// dato il numero di purchases per ogni coppia date_bid, ricavo:
		// BID, (date, dailyPurchases)
		// nascondo dietro al BID il numero di acquisti per ogni giorno
		JavaPairRDD<String, DateNumPurchases18> dailyForEachBID = sumCtrRDD.
		mapToPair((Tuple2<String, Integer> entry) -> {
			String[] fields = entry._1().split("_");
			String date = fields[0];
			String BID = fields[1];

			return new Tuple2<String, DateNumPurchases18>
			(BID, new DateNumPurchases18(date, entry._2()));

		});

		// now we can join
		// result:
		// BID, ((date, numPurchases18), totalNumberOfPurchases2018forABook)
		JavaPairRDD<String, Tuple2<DateNumPurchases18, Integer>> joinResultRDD =
		dailyForEachBID.join(totPurchasesForEachBidRDD);

		// === now we have to count ===
		// general schema:
		// filter -> flatMapToPair -> reduceByKey -> filter -> save

		//*filter: dailyPurchases>0.1*totalNumberOfPurchases2018forABook
		//*flatMapToPair: 
		// 	For each input pair return three pairs
		// 	- (bid+date, +1)
		// 	- (bid+date-1, +1)
		// 	- (bid+date-2, +1)
		//*reduceByKey: sum the value of occurrences of each bid+date entry
		//*filter: numOccurrences > 3
		//	that means three consecutive days are good
		//*save

		//getting only the entries where
		//dailyPurchases18 > 0.1 * totalNumberOfPurchases2018forABook
		JavaPairRDD<String, Tuple2<DateNumPurchases18, Integer>> filteredResult = 
		joinResultRDD.filter((entry) -> {
			int dailyPurchases18 = entry._2()._1().numPurchases18;
			int totalNumberOfPurchases2018forABook = entry._2()._2();

			if (dailyPurchases18 > 0.1 * totalNumberOfPurchases2018forABook)
				return true;
			else
				return false;
		});

		// For each input pair return threee pairs
		// 	- (bid+date, +1)
		// 	- (bid+date-1, +1)
		// 	- (bid+date-2, +1)
		JavaPairRDD<String, Integer> pairsDatesRDD = filteredResult.
		flatMapToPair((Tuple2<String, Tuple2<DateNumPurchases18, Integer>>
		entry) -> {
			ArrayList<Tuple2<String, Integer>> elements = new ArrayList<String, Integer>();

			String bid = entry._1();
			String date = entry._2()._1().date;

			String yesterday = DateTool.previousDeltaDate(date, 1);
			String twoPrevious = DateTool.previousDeltaDate(date, 2);

			elements.add(new Tuple2<String, Integer>(bid+","+date, new Integer(1)));
			elements.add(new Tuple2<String, Integer>(bid+","+yesterday, new Integer(1)));
			elements.add(new Tuple2<String, Integer>(bid+","+twoPrevious, new Integer(1)));

			return elements.iterator();
		});

		// summing the total number of occurrences for each pair bid+date
		JavaPairRDD<String, Integer> totalSumRDD = pairsDatesRDD.reduceByKey(
		(v1, v2) -> v1 + v2);

		// filtering all values in order to take only numOccurrences > 3
		JavaPairRDD<String, Integer> filteredTotalSumRDD = totalSumRDD.fitler(entry -> {
			int numOccurrences = entry._2();

			return numOccurrences == 3 ? true : false;
		});

		filteredTotalSumRDD.keys().saveAsTextFile(outputPathPart2);

		sc.close();

	}
}
