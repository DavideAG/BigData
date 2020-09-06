/* Lecture 23 - Exam, spark phase */

// Consedering statistics related to May 2018, select only yhe VSID_hour for
// which the average of CPUUsage% if greather than a threshold (CPUthr) and
// RAMUsage% is greather than RAMthr
//
// 	Timestamp,VSID,CPUUsage%,RAMUsage%
//
// Timestamp is like	2018/03/01,15:40


//	high level schema
// textFile		-> nomeFile
// filter		-> may 2018	CACHE
// mapToPair		-> VSID_hour, (CPUUsage%, RAMUsage%, 1)
// reduceByKey		-> VSID_hour, (totCPUUsage%, totRAMUsage%, totNumOfLines)
// mapToPair		-> VSID_hour, (avgCPUUsage%, avgRAMUsage%)
// filter		-> avgCPUUsage% > CPUthr% && avgRAMUsage% > RAMthr%
// saveAsTextFile(keys())


//	high level schema
// from filtered may 2018
// mapToPair		-> VSID_date_hour, CPUUsage%
// reduceByKey		-> VSID_date_hour, MAX_CPUUsage%
// filter		-> MAX_CPUUsage% > 90 || MAX_CPUUsage% < 10
// mapToPair		-> VSID_date, (occurr>90 [0/1], occurr<10[0/1])
// reduceByKey		-> VSID_date, (totOccurr>90, totOccurr<10)
// filter		-> totOccurr>90 > 8 && totOccurr<10 > 8
// saveAsTextFile
