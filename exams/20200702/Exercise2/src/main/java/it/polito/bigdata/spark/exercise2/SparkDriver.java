package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPathServers;
		String inputPathPatchesServers;
		String outputPathPart1;
		String outputPathPart2;

		inputPathServers = "exam_ex2_data/Servers.txt";
		inputPathPatchesServers = "exam_ex2_data/PatchedServers.txt";
		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").
		setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* Write your code here */

		JavaRDD<String> patchedRDD = sc.textFile(inputPathPatchesServers);

		JavaRDD<String> filteredRDD = patchedRDD.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[2];

			if (date.startsWith("2018") || date.startsWith("2019"))
			{
				return true;
			} else {
				return false;
			}
		}).cache();

		JavaPairRDD<String, Count1819> firstStepCounterRDD = filteredRDD.
		mapToPair(line -> {
			String[] fields = line.split(",");
			String sid = fields[0];
			String date = fields[2];
			Count1819 ctr;

			if (date.startsWith("2018"))
				ctr = new Count1819(1, 0);
			else
				ctr = new Count1819(0,1);

			return new Tuple2<String, Count1819>(sid, ctr);
		});

		JavaPairRDD<String, Count1819> secondStepCounterRDD = firstStepCounterRDD.
		reduceByKey((c1, c2) -> {
			c1.count18 += c2.count18;
			c1.count19 += c2.count19;

			return c1;
		});

		JavaPairRDD<String, Count1819> onlyOkRDD = secondStepCounterRDD.
		filter(entry -> {
			return entry._2().count19 > 0.5 * entry._2().count18 ? true : false;
		});

		JavaRDD<String> serversRDD = sc.textFile(inputPathServers);

		JavaPairRDD<String, String> serversPairRDD = serversRDD.
		mapToPair(line -> {
			String[] fields = line.split(",");

			String sid = fields[0];
			String model = fields[1];

			return new Tuple2<String, String>(sid, model);
		}).cache();

		JavaPairRDD<String, Tuple2<String, Count1819>> joinedRDD = serversPairRDD.
		join(onlyOkRDD);

		JavaPairRDD<String, String> mappedValuesRDD = joinedRDD.
		mapValues(pair -> {
			String model = pair._1();
			return model;
		});

		mappedValuesRDD.saveAsTextFile(outputPathPart1);


		/* part B */

		JavaPairRDD<String, Integer> firstCounterSidDateRDD = filteredRDD.
		mapToPair(line -> {
			String[] fields = line.split(",");

			String sid = fields[0];
			String date = fields[2];

			return new Tuple2<String, Integer>(sid + "_" + date, new Integer(1));
		});

		JavaPairRDD<String, Integer> secondCounterSidDateRDD =
		firstCounterSidDateRDD.reduceByKey((v1, v2) -> v1 + v2);

		JavaPairRDD<String, Integer> filteredCounterSidDateRDD =
		secondCounterSidDateRDD.filter(entry -> {
			return entry._2() > 1? true : false;
		});

		JavaPairRDD<String, String> pairResRDD =
		filteredCounterSidDateRDD.mapToPair(entry -> {
			String sid = entry._1().split("_")[0];
			return new Tuple2<String, String>(sid, "");
		});

		JavaPairRDD<String, String> resSub = serversPairRDD.
		subtractByKey(pairResRDD);

		resSub.saveAsTextFile(outputPathPart2);

		System.out.println("Number of distinct models = " + resSub.values().
		distinct().count());

		sc.close();
	}
}
