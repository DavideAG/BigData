import java.io.Serializable;

package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Ctr implements Serializable {
    public Double maxTmp;
    public int numOccurrences;
    
    public Ctr(Double maxTmp, int numOccurrences) {
        this.maxTmp = maxTmp;
        this.numOccurrences = numOccurrences;
    }
    
}