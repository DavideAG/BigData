/* Exam 22-01-2018 */
// Q1	-> A: B
// Q2	-> A: B

/* Input files:
 * 	Books.txt
 * 		bid,title,genre,publisher,year_publication
 *
 * 	Purchases.txt
 * 		customerId,bid,timestamp.price
 *
 * 		timestamp example	=	20170502_13:10
 */

protected void map(LongWritable key,
		Text value,
		Context context) {
	String[] fields = value.toString().split(",");
	String bid = fields[1];
	String date = fields[2].split("_")[0];
	if (date.startWirth("2016"))
		context.write(new Text(bid), new IntWritable(1));
}



String mostPurchased;
Integer numPurchases
protected void setup(Context context) {
mostPurchased = new String("ZZZ");
numPurchases = Integer.MIN_VAL;
}

protected void reduce(Text key,
		Iterable<IntWritable> values,
		Context context) {
	Integer ctr = new Integer(0);
	String bid = key.toString();

	for (IntWritable val : values) {
		ctr += val.get();
	}

	if (ctr > numPurchases) {
		numPurchases = ctr;
		mostPurchased = bid;
	} else if ((ctr.compareTo(numPurchases) == 0) &&
		  (bid.compareTomostPurchased) < 0) {
		mostPurchased = bid;	  
	}
}

protected void cleanup(Context context) {
	context.write(new Text(mostPurchased), new IntWritable(numPurchases));
}

// Part II
/* Input files:
 * 	Books.txt
 * 		bid,title,genre,publisher,year_publication
 *
 * 	Purchases.txt
 * 		customerId,bid,timestamp,price
 *
 * 		timestamp example	=	20170502_13:10
 */

// 1 - high level schema
// textFile		-> Purchases.txt
// filter		-> year = 2017		CACHE
// mapToPair		-> bid, (price, price)
// reduceByKey		-> bid, (maxPrice, minPrice)
// filter		-> maxPrice - minPrice > 15
// override toString method like
// 	public String toString() {
//		return new String(maxPrice + "," + minPrice);
// 	}
// saveAsTextFile 


//	percentage of never purchased books per genre
// 2 - high level schema
// from cached file
// map			-> bid
// distinct	THE RESULT IS X	(Purchased in 2017)
//
// textFile		-> Books.txt		CACHE
// mapToPair		-> bid, genre
//		THE RESULT IS Y
// mapToPair		-> genre, 1
// reduceByKey		-> genre, totBooksPerGenre
// 
// subtract		-> Y - X
// 		THE RESULT IS Z		(bid, genre)	never purchased in 2017
//
// Z.mapToPair		-> genre, 1
// reduceByKey		-> genre, totNeverSoldPerGenre
// join			-> genre (totNeverSoldPerGenre, totBooksPerGenre)
// mapValues		-> totNevelSoldPerGenre / totBooksPerGenre

public static void main(String[] args) {

                String inputPathBooks;
                String inputPathPurchases;
                String outputPathPartA;
                String outputPathPartB;

                inputPathBooks = args[0];
                inputPathPurchases = args[1];
                outputPathPartA = args[2];
                outputPathPartB = args[3];

                // Create a configuration object and set the name of the application
                SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_01_22 - Exercise #2");

                // Create a Spark Context object
                JavaSparkContext sc = new JavaSparkContext(conf);

		// part I
		JavaRDD<String> purchasesRDD = sc.textFile(inputPathPurchases);

		JavaRDD<String> filtered7RDD = purchasesRDD.filter(line -> {
			String[] fields = line.split(",");
			String timestam = fields[2];
			return timestamp.startsWith("2017");
		}).cache();

		JavaPairRDD<String, maxMinPrice> pairRDD = filtered7RDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String bid = fields[0];
			Double price = Double.parseDouble(fields[3]);
			return new Tuple2<String, maxMinPrice>(bid, new maxMinPrice(price, price));
		});

		JavaPairRDD<String, maxMinPrice> maxMinPair = pairRDD.redueByKey((mm1, mm2) -> {
			maxMinPrice mm_tmp = new maxMinPrice(Double.MIN_VAL, Double.MAX_VAL);

			if (mm1.maxPrice > mm2.maxPrice)
				mm_tmp.maxPrice = mm1.maxPrice;
			else
				mm_tmp.maxPrice = mm2.maxPrice;

			if (mm1.minPrice < mm2.minPrice)
				mm_tmp.minPrice = mm1.minPrice;
			else
				mm_tmp.minPrice = mm2.minPrice;

			return mm_tmp;
		});


		JavaPairRDD<String, maxMinPrice> filteredMaxMinPrice = maxMinPair.filter(pair -> {
			return (pair._2().maxPrice - pair._2().minPrice) > 15;
		});

		filteredMaxMinPrice.saveAsTextFile(outputPathPartA);
		
		
		// part II
		JavaRDD<String> bids = filtered7RDD.map(line -> {
			String[] fields = line.split(",");
			String bid = fields[0];
			return bid;
		});

		JavaRDD<String> purchasedIn17 = bids.distinct();

		JavaRDD<String> books = sc.textFile(inputPathBooks);

		JavaPairRDD<String, String> bidGenreRDD = books.mapToPair(line -> {
			String[] fields = line.split(",");
			return new Tuple2<String, String>(fields[0], fields[2]);
		});

		JavaPairRDD<String, Integer> firstCountPair = books.mapToPair(line -> {
			String[] fields = line.split(",");
			String genre = fields[2];
			return new Tuple2<String, Integer>(genre, new Integer(1));
		});

		JavaPairRDD<String, Integer> secondCountPair = firstCountPair.reduceByKey((v1, v2) -> v1 + v2);

		JavaPairRDD<String, String> neverPurchasedIn17 = bidGenreRDD.subtract(purchasesIn17);

		JavaPairRDD<String, Integer> neverPurchased17CounterFirst = neverPurchasedIn17.mapToPair(pair -> {
			String genre = pair._2();
			return new Tuple2<String, Integer>(genre, new Integer(1));
		});

		JavaPairRDD<String, Integer> neverPurchased17CounterSecond = neverPurchased17CounterFirst.reduceByKey(
				(v1, v2) -> v1 + v2);

		JavaPairRDD<String, Tuple2<Integer, Integer>> neverPurchased17CounterSecond.join(secondCountPair);

		JavaPairRDD<String, Double> genrePercentageRDD = neverPurchased17CounterSecond.mapValues(pair -> {
			Integer totNeverSoldPerGenre = pair._1();
			Integer totBooksPerGenre = pair._2();

			return new Double(totNeverSoldPerGenre / totBooksPerGenre);
		});

		genrePercentageRDD.saveAsTextFile(outputPathPartB);
}

