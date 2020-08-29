# BigData
**Author:** Davide Antonino Giorgio - s262709  
**Professor:** Paolo Garza  
**Year:** 2019-2020

---
<p><img align="right" src="./hadoop.png" width="150px"/></p>
<p></p>

## MapReduce

Map reduce is a way to analyze BigData and solve problems like the word count.
Data are stored in chunks (usually of 64-128 MB) and they are replicated inside a **HDFS**. (Hadoop Distributed File System). HDFS is built to be **failure tolerant**.
In an Hadoop cluster, one node is designed has **master node** that is used to store information related to the mapping between the each chunk name and where it is physically located. 

The **Mapper** is used to process a chunk of data and emit the *(key, value)* pairs to sent to the Reducer through the network. Some [pre-aggregation](#Combiner) can be performed to limit the amount of the network data. The number of instances of the mapper classes are defined by the number of chunks to analyze. The system automatically defines it.



### Example 1:

```
file1.txt	->	256MB
file2.txt	->	257MB

block chunk size 128MB
```

```
file1.txt	->	256MB / 128MB	->	2 chunks
file2.txt	->	257MB / 128MB	->	3 chunks

Total number of chunks	->	5
```

the number of the mapper instances for our application will be equal to the total number of chunks needed to store our files: `5`



### Example 2:

```
file1.txt	->	255MB
file2.txt	->	257MB

block chunk size 256MB
```

```
file1.txt	->	255MB / 256MB	->	1 chunks
file2.txt	->	257MB / 256MB	->	2 chunks

Total number of chunks	->	3
```

the number of the mapper instances for our application will be equal to the total number of chunks needed to store our files: `3`

Different files can't be stored inside the same chunk.
Replicants are not considered during the definition of the number of mapper classes instances.





## Combiner

***Combiners*** are **used to pre-aggregate data** in the main memory of the single node before sending them to the reducer through the network.
Combiners work on the keys that are emitted by the mapper.

Combiners work only if the reduce function is **associative** and **commutative**.
The execution of the combiner is not guaranteed in fact, Hadoop optimizer **can decide to not execute a combiner**.

> **NB**: due to the fact that the combiner phase can not be executed, the output emitted of the mapper and the reducer must be consistent, otherwise sometimes the system will raise an error.

Combiners are characterized by the presence of the `reduce()` method.
The Combiner class is specified by using the `job.setCombinerClass()` method in the run method of the Driver. The parameter of this method is the name of the combiner class. 

### Example of combiner in the word count problem:

```java
// in the driver class

// Set combiner class
job.setCombinerClass(WordCountCombiner.class);
```

In this case we decided to create a new class for the combiner but this is not necessary. because the `reducer` method can be implemented in a different position.

No modification will be applied to the the `mapper` and the `reducer`.
The implementation of the combiner class can be as follow:

``` java
/* Combiner Class */
class WordCountCombiner extends Reducer<
    Text,			// Input key type
	IntWritable,	// Input value type
	Text,			// Output value type
	IntWritable>	// Output value type
    {
        
        /* Implementation of the reduce method */
        protect void reduce(
        	Text key,						// Input key type
        	Iterable<IntWritable> values,	// Input value type
        	Context context) throws IOException, InterruptedException {
            
            int occurrences = 0;
            
            // Iterate over the set of values and sum them
            for (IntWritable value:values) {
                occurrences = occurrences + value.get();
            }
            
            // Emit the total number of occurrences of the current word
            context.write(key, new IntWritable(occurrences));
        }
    }
```

In this case it is performed the same operation that is present in the reducer because it is the right operation to do in the word count problem. For each key one single key-value pair is emitted that contains for each word the total number of occurrences.

> **NB**: If the combiner and the reducer performs the same operation like in the word count problem, it is possible to specify the reducer class as also combiner class.
> In 99% of the Hadoop applications the same class can be used to implement both combiner and reducer.

```java
// in the driver class

// Set combiner class using the reducer
job.setCombinerClass(WordCountReducer.class);
```





## Personalized Data Types

### for the value part

Personalized data types are useful when we want to define `value` of a key-value pair where it is a complex data type. It is necessary to implement the interface `org.apache.hadoop.io.Writable` implementing the following methods:

``` java
public void readFields(DataInput in)	// Deserialize the fields of this object from in
public void write(DataOutput out)		// Serialize the fields of the object out
```

usually is also redefined the method `public String toString()` to properly format the output of the job.

> **NB**: the only one part that is required at the exam is like the one shown as follow:

``` java
public class sumAndCountWritable implements org.apache.hadoop.io.Writable {
    /* Private variables */
    private float sum = 0;
    private int count = 0;
    
    ...
}
```

So just the part related to the values that defines out personalized data type.

### for the key part

In this case the personalized data type class has to extend the `org.apache.io.WritableComparable` interface. You have to implement the following methods:

- `compareTo()`
  - defines how to compare two objects
- `hashCode()`
  - used to split keys in group to send to a dedicated reducer





## Sharing parameters among Driver, Mappers, and Reducer

Sometimes is necessary to share a parameter among Driver, Mappers and Reducer. For example, we can think to pass a value, that will be used as threshold by the mappers, through the command line to the Driver.

Personalized (property-name, property value) pairs are useful to shared *small* (constant) properties that are available only during the execution of the program.

The `driver` set them:

``` java
Configuration conf = this.getConf();	// retrive the configuration object
conf.set("property-name", "value");		// set personalyzed properties
```

> **NB**: `conf.set` must be invoked before creating the job object.

In the `Mapper` or in the `Reducer`:

``` java
// returns a string containing the value of the specified property
context.getConfiguration().get("property-name");
```





## Counters

All the statistics that are shows after the execution of a job in a Map Reduce application are called *counters*. A counter is an integer variable that is initialized in the driver and then it can be incremented in the map method or in the reduce method.

It can be used to implement global statistics.
For example we can think to count the number of total input records that are not well formatted.
We have to check if the format is correct for each record or the system will raise a runtime error during the execution.

> **NB**: The only operation permitted is an **increment**.

A counter is defines by means of `Java enum` (arbitrary number). It can be incremented by the mapper or the reducer by using the `increment()` method.

``` java
// in the mapper or in the reducer for increment the value of a counter
context.getCounter(<countername>).increment(<value>);
```

The `getCounters()` and `findCounter()` methods are used by the Driver to retrieve the final values of the counters.
The name of the enum is the group name.

``` java
// in the Driver
// this enum defines two counters
public static enum COUNTERS {
    ERROR_COUNT,
    MISSING_FIELDS_RECORD_COUNT
}

// to retrieve the value of the counter ERROR_COUNT in the driver
job.getCounters().findCounter(COUNTERS.ERROR_COUNT);
```





## Map only Jobs

Sometimes we don't really need both phases and we can put the number of the reducer = 0.
For example, select the number of sensors with PM10 value greater than the threshold.
Data coming from the mapper are not sent to the network but are stored directly in the Hadoop Distributed File System.
The combiner is not necessary too in a map-only job.

`NullWritable` object is a specific object used to write something that is empty.

``` java
// in the Driver
job.setNumReducerTasks(0);
```





## Setup and cleanup method of the Mapper

The `setup` method is called once for each mapper prior to the many calls to the map method. It can be used to set the values of in-mapper variables. In-mapper variables are used to maintain in-mapper statistics and preserve the state (locally for each mapper) within and across calls to the map method.

The `cleanup` method is called once for each mapper after the many calls to the map method. It can be used to emit (key,value) pairs based on the values of the in-mapper variable/statistics.

> **NB**: they are empty if not override.

The same  methods are available fro the *reducer* class:

- The setup method is called once for each reducer prior to many calls of the reduce method.
- The cleanup method is called once for each reducer after the many calls of the reducer method.





## In-mapper combiner

In-mapper combiner is a possible improvement over standard combiners. The concept can be resumed in three distinct points:

- Initialize a set of in-mapper variables during the instance of the Mapper.
  It must be done in the *setup* method of the mapper class.

- Then in the map method these in-mapper variables are updated.
  Usually no key-value pairs are emitted in the map method.

- Finally a set of key-value pairs are emitted in the cleanup method of the mapper, based on the values of the in-mapper variables.

The goal is to do the same operations of a combiner without creating a combiner object.

**PROBLEM**: *Out of memory* problem because the hash map used as local variable can be potentially big.





## MapReduce design patterns

There are several map reduce patters:



### Summatization Patterns

#### Numerical Summarization (Statistics)

Are used to implement applications that produce top-level/summarized view of the data.
The **goal** is to group records/objects by a key field and calculate a numerical aggregate (average, max, min, standard deviation, ...) per group.
It provide a top-level view of large input data sets.

The **structure** of the summarization pattern is:

- **Mappers**: output (key, value) pairs where
  - key is associated with the fields used to define **groups** (e.g., sensorId)
  - value is associated with the fields used to compute the aggregate statistics (e.g., PM10value)
- **Reducers**: receive a set of numerical values for each "*group-by*" key and compute the final statistics for each "group"
- **Combiners**: If the computed statistics has specific properties (e.g., it is associative and commutative), combiners can be used to speed up performances.



Known uses:

- word count
- record count
- min/max count
- average/median/standard deviation





#### Inverted index summarizations

The **goal** of this pattern is to build an index from the input data to support faster searches or data enrichment (create the index which will be used to query the data).
Map terms to a list of identifiers done to improve search efficiency.

The **structure** of the inverted index summarization pattern is:

- **Mappers**: output (key, value) pairs where
  - key is the set of fields to index (a keyword)
  - value is a unique identifier of the objects to associate with each keyword (e.g., the URL of the keyword that we are analyzing in the map phase)
- **Reducers**: receive a set of identifiers for each keyword and simply concatenate them
- **Combiners**: Usually are not useful when using this pattern, because there is no values to aggregate. Combiners are useful if I have repetitions in the mapper phase (e.g. word 'data' more times in the same web page).



Known uses:

- web search engine
  (word, list of URLs that contains the word)





#### Counting with counters

The **goal** of this pattern is to compute count summarizations of data sets.
It provides a top-level view of a large data sets.
Usually is used inside other types of pattern or to output very simple statistics.

The **structure** of the inverted index summarization pattern is:

- **Mappers**: process each input record and increment a set of counters
- **Map-only job**: no reducers, no combiners
- **Driver**: stores the results of the application and prints them



Known uses:

- Count number of records
- count a small number of unique instances
- summarizations



---

### Filtering Patterns



#### Filtering - (map-only job, no reducers)

The **goal** is to filter out input records that are not of interest/keep only the ones that are of interest. Focus the analysis of the records of interest.

The **structure** of the filtering pattern is:

- **Input**: Key = primary key, value = record
- **Mappers**: output one (key, value) pair for each record that satisfies the enforced filtering rule
  - Key is associated with the primary key of the record
  - Value is associated with the selected record
- **Reducers**: The reducer is useless in this pattern --> A **map-only job** is executed (number of reduce set to 0)



Know uses:

- record filtering
- tracking events
- distributed grep
- data cleaning





#### Top K - (N mappers and 1 reducer)

The **goal** is to select a small set of top K records according to a ranking function. Focus on the most important records of the input data set.

The **structure** of Top K pattern is:

- **Mappers** each mapper initializes an in-mapper top k list

  - k is usually small (e.g., 10)
  - The current top k-records of each mapper can be stored in main memory
  - Initialization performed in the setup method of the mapper

  The map function updates the current in-mapper top k list.
  The cleanup method emits the k (key, value) pairs associated with the in-mapper local top k record

  - Key is the null key (*NullWritable*)
  - Value is a in-mapper top k record

- **Reducer**: *A single reducer is instantiated*, it computes the final top k list by merging the local list emitted by the mappers.

  - All input (key, value) pairs have the same key, so the reduce method is called only once



Know uses:

- outliers analysis
- select interesting data





#### Distinct

The **goal** is to find a unique set of values/records.

The **structure** of distinct pattern is:

- **Mappers**: Emit one (key, value) pair for each input record
  - **Key** = input record
  - **Value** = null value *NullWritable*
- **Reducers**: Emit one (key, value) pair for each input (key, list of values) pair
  - **Key** = input key, i.e., input record
  - **Value** = null value *NullWritable*



Know uses:

- duplicate data removal
- distinct value selection



---

## Multiple Inputs

Data read from multiple input folders.
*Hadoop* allows reading data from multiple inputs with different formats:

- One different mapper for each input dataset must be specified
- However, the key-value pairs emitted by the mappers must be consistent in terms of data types.

In the Driver I must add the method `addInputPath` of the `MultipleInputs` class **multiple times** to:

- add one input path at a time
- specify the input format class for each input path
- specify the Mapper class associated with each input path

Instead of the `setMapperClass` :

```java
// First input path for the first folder
MultipleInputs.addInputPath(job,
                            new Path(args[1]),
                            TextInputFormat.class,	// can be different
                            Mapper1.class);	// mapper class to analyze data

// Second input path for the second folder
MultipleInputs.addInputPath(job,
                            new Path(args[2]),
                            TextInputFormat.class,	// can be different
                            Mapper2.class);	// mapper class to analyze data
```

repeat this for all the mapper classes that I want to create.

For the data types of the emitted (key, value) pairs use the standard methods showed before.

> **NB**: the data types of the emitted (key, value) pairs are independent of the mapper class.
> The input are characterized by different data formats but the output has the same data format.



## Multiple outputs

In some applications it could be useful to store the output key-value pairs of a MapReduce application in different files.

- Each file contains a specific subset of the emitted key-value pairs (based on some rules)
- Usually this approach is useful for splitting and filtering operations
- Each file name has a prefix that is used to specify the content of the file

Usually when we want to split the data and store some part in a file with a certain prefix and the other part in a file with a different prefix.

In the Driver use the method `MultipleOutputs.addNamedOutput` for every prefix specifies:

- The job object
- The name/prefix of MultipleOutputs
- The OutputFormat class
- The key output data type class
- The value output data type class

```java
MultipleOutputs.addNamedOutput(job,
                              "hightemp",
                              TextOutputFormat.class,
                              Text.class,
                              NullWritable.class);

MultipleOutputs.addNamedOutput(job,
                              "normaltemp",
                              TextOutputFormat.class,
                              Text.class,
                              NullWritable.class);

```



In the `setup method of the Reducer` (or Mapper if Map-only job) we need to create an object of the `MultipleOutputs` type.

```java
// to define in the reducer (or mapper if map-only job)
private MultipleOutputs<Text, NullWritable> mos = null;

// to be defined in the setup method of the reducer (or mapper)
mos = new MultipleOutputs<type of key, type of value>(context);
```

To write inside a file we have to use something like that:

``` java
mos.write("hightemp", key, value);		// to write in 'hightemp' file

mos.write("normaltemp", key, value);	// to write in 'normaltemp' file
```

> **NB**: Remember to close the `MultipleOutputs` object in the `cleanup` method of the reducer (or in the mapper if map-only job)

```java
mos.close();	// used to close the mos object
```



---

## Distributed cache

Sometimes we want to share some information between the mapper and the reducer. It is possible use [properties](#Sharing parameters among Driver, Mappers, and Reducer) but only for small values. If we want to share more information like text files or libraries we can use ***distributed cache*** that is a mechanism.

In the Driver we need to specify:

```java
// in the run method of the driver
job.addCacheFile(new Path("hdfs_path").toUri());
```

The shared/cache file is read by the mapper/reducer usually in its *setup* method.

**NB**: since the shared/cache file is available **LOCALLY** (because Hadoop creates a **local copy** of it in all nodes) in the node, it's content can be read **efficiently**.

To retrieve the info about the file in the mapper/reducer

```java
// usually in the setup method of the reducer of the mapper
URI[] usrisCahedFiles = context.getCachedFiles();
```

To **read** the content of the file given its uri:

```java
Bufferedreader file = new BufferedReader(new FileReader(new
          	                  File(urisCachedFiles[0].getPath())));

while((line = file.readLine()) != null) {
    // process current line
}
	...
file.close();
```

---

## Data Organization Patterns

Are used to reorganize/split in subsets the input data.
The output of an application based on an organization pattern is usually in the input of other application(s).

### Binning

The goal of this pattern is to Organize/move the input records into categories.

The idea is to partition a big data set into distinct, smaller data sets (bins) containing similar records. Each partition is usually the input of a following analysis.

The structure of this pattern is the following one:

- Based on a Map-only job
- Driver sets the list of bins/output files by means of MultipleOutputs

Tasks achieved by the mapper hare:

- For each input (key. value) pair, select the output bin/file associated with it and emit a (key, value) in that file:

  - key of the emitted pair = key of the input pair
  - value of the emitted pair = value of the input pair

  No combiner or reducer is used in this pattern

### Shuffling

The goal of this pattern is randomize the order of the data (records)

It is used for anonymization reasons or for selecting a subset of random data (records).

The structure is the following one:

- Mappers emit on (key, value) or each input record
  - key is a random key (i.e., a random number)
  - value is the input record
- Reducers emit one (key, value) pair for each value in [list-ofvalues] of the input (key, [list-of-values]) pair.



## Metapatterns

Is a pattern containing a set of jobs.

### Job Chaining

The goal is to execute a sequence of jobs (synchronizing them).

The use is to manage the workflow of complex applications based on many phases (iterations).

- Each phase is associated with a different MapReduce Job (i.e., one sub-application)
- The output of a phase is the input of the next one

About the structure:

- The (single) driver

  - contains the workflow of the application
  - executes the jobs in the proper order

  **NB**: Specify how many jobs and in wich order

  Mappers, reducers, and combiners

  - Each phase of the complex application is implemented by a MapReduce Job.
    i.e., it is associated with a mapper, a reducer (and a combiner if it is useful)



## Join Patterns

Used when I need to join data from two different files/tables
It's possible to implement each type of join (natural, theta, semi, outer)
**NOTE**: to use the other joins (different from the natural one) I need to change the reduce method 

- Reduce side join
	**GOAL** =  Join the content of two relations (i.e., relational tables)
	**STRUCTURE**: 
		There are two mapper classes (One mapper class for each table)
		Mappers emit one (key, value) pair for each input record
	- Key is the value of the common attribute(s)
	- Value is the concatenation of the name of the table of the current record and the content of the current record

Reducers iterate over the values associated with each key (value of the common attributes) and compute the local natural join for the current key

- Generate a copy for each pair of values such that one record is a record of the first table and the other is the record of the other table
- The system invokes the reducer one time for each key 
  **LIMIT** = iterate 2 times, so potentially an out of memory error a lot of data in the network
  More efficient solution = map only job --> mapper reads one record at a time and then join the current record with all the records of the small table (stored in the main memory) use a distributed cache 

#### Theta-join
specify the condition and which are the attributes on which I want to perform the condition

#### Semi-join
just emits the solution relative to one of the 2 tables



## Relational Algebra Operators

