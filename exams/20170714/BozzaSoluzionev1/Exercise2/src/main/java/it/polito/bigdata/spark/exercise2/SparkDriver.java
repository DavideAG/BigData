package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathTemperatures;
		String outputPathPartA;
		String outputPathPartB;

		inputPathPrices = args[0];
		outputPathPartA = args[1];
		outputPathPartB = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().
		setAppName("Spark Exam 2016_07_14 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Reading the content of the file
		JavaRDD<String> temperaturesRDD = sc.textFile(inputPathTemperatures);

		// Getting only summer 2015
		JavaRDD<String> summer2015RDD = temperaturesRDD.filter((String line) -> {
			String[] fields = line.split(",");

			//fields[0] is the date
			if (fields[0].compareTo("2015/06/01") > 0 &&
				fields[0].compareTo("2015/08/31") < 0)
			{
				return true;
			} else {
				return false;
			}
		});

		// mapping city-country and max temperature
		JavaPairRDD<String, Ctr> pairCityCountryFirstRDD = summer2015RDD.mapToPair(
			(String line) -> {
			
			String[] fields = line.split(",");
			String city = fields[1];
			String country = fields[2];
			Double maxTmp = Double.parseDouble(fields[3]);

			Ctr counter = new Ctr(maxTmp)
		});






















		// Read the content of Temperatures.txt
		JavaRDD<String> temperatures = sc.textFile(inputPathTemperatures);

		// Select data of year summer 2015
		JavaRDD<String> tempSummer2015 = temperatures.filter(new YearSummer2015());

		// Extract (city_county, maxTemp_1)
		JavaPairRDD<String, MaxTempCount> cityCountryMaxTempCount = tempSummer2015.mapToPair(new CityCountryMaxTemp());

		// Sum temps and counts
		JavaPairRDD<String, MaxTempCount> cityCountrySumMaxTempCount = cityCountryMaxTempCount
				.reduceByKey(new SumTempCount()).cache();

		// Store the results of the first part
		cityCountrySumMaxTempCount.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Extract (country, MaxTempCount city of that country)
		JavaPairRDD<String, MaxTempCount> countryMaxTempCount = cityCountrySumMaxTempCount
				.mapToPair(new CountryMaxTemp());

		// Sum temps and counts
		JavaPairRDD<String, MaxTempCount> countrySumMaxTempCount = countryMaxTempCount.reduceByKey(new SumTempCount());

		// Map cityCountySumMaxTempCount to (country, city_sumtemps_count)
		JavaPairRDD<String, CityMaxTempCount> county_CitySumMaxTempCount = cityCountrySumMaxTempCount
				.mapToPair(new CountryCityStats());

		// Join
		JavaPairRDD<String, Tuple2<CityMaxTempCount, MaxTempCount>> risJoin = county_CitySumMaxTempCount
				.join(countrySumMaxTempCount);

		// Filter average city > average country
		JavaPairRDD<String, Tuple2<CityMaxTempCount, MaxTempCount>> filteredRisJoin = risJoin.filter(new HotCity());

		// Extract city
		JavaRDD<String> hotCities = filteredRisJoin.map(new City());

		hotCities.saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
