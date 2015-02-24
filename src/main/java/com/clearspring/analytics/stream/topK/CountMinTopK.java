package com.clearspring.analytics.stream.topK;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tamara on 2/18/2015.
 */
public class CountMinTopK implements TopK {

    CountMinSketch countMinSketch;
    HashMap<Object,Long> topK;
    double phi;
    long size;

    public CountMinTopK(CountMinSketch countMinSketch, double phi){
        this.countMinSketch = countMinSketch;
        this.size = 0;
        this.phi = phi;
        this.topK = new HashMap<Object,Long>();
    }

    public void addLong(long item, long count) {
        countMinSketch.add(item,count);
        size+=count;
        updateTopK(item);
    }

    public void addString(String item, long count) {
        countMinSketch.add(item,count);
        size+=count;
        updateTopK(item);
    }

    @Override
    public void addObject(Object o) {
        long objectHash = MurmurHash.hash(o);
        countMinSketch.add(objectHash, 1);
        size+=1;
        updateTopK(objectHash);
    }

    private void updateTopK(long item){
        long minFrequency = (long)Math.ceil(size*phi);
        long estimateCount = countMinSketch.estimateCount(item);
        if (estimateCount >= minFrequency){
            topK.put(item,estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void updateTopK(String item){
        long minFrequency = (long)Math.ceil(size*phi);
        long estimateCount = countMinSketch.estimateCount(item);
        if (estimateCount >= minFrequency){
            topK.put(item,estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void removeNonFrequent(long minFrequency){
        ArrayList<Object> nonFrequentKeys = new ArrayList<Object>();
        for (Map.Entry<Object, Long> entry : topK.entrySet()){
            if(entry.getValue()<minFrequency){
                nonFrequentKeys.add(entry.getKey());
            }
        }
        for (Object o:nonFrequentKeys){
            topK.remove(o);
        }
    }

    public static CountMinTopK merge(CountMinTopK... estimators) throws CMTopKMergeException {
        CountMinSketch[] countMinSketches = new CountMinSketch[estimators.length];
        CountMinSketch mergedSketch;

        for (int i=0;i<estimators.length;i++){
            countMinSketches[i] = estimators[i].countMinSketch;
        }
        try {
            mergedSketch = CountMinSketch.merge(countMinSketches);
        }catch (Exception ex){
            throw new CMTopKMergeException("Cannot merge count min sketches: "+ex.getMessage());
        }

        double phi = estimators[0].phi;
        Map<Object,Long> topK = new HashMap<Object,Long>();
        CountMinTopK mergedTopK = new CountMinTopK(mergedSketch,phi);

        if (estimators != null && estimators.length > 0) {
            for (CountMinTopK estimator : estimators) {
                if (estimator.phi != phi) {
                    throw new CMTopKMergeException("Frequency expectation cannot be merged");
                }
                for (Map.Entry<Object, Long> entry : estimator.topK.entrySet()) {
                    if (topK.containsKey(entry.getKey())){
                        topK.put(entry.getKey(),mergedSketch.estimateCount((long)entry.getKey()));
                    }else{
                        topK.put(entry.getKey(),entry.getValue());
                    }
                }
            }
        }
        long minFrequency = (long) Math.ceil(mergedTopK.size * estimators[0].phi);
        mergedTopK.topK = new HashMap<Object,Long>(topK);
        mergedTopK.removeNonFrequent(minFrequency);
        return mergedTopK;
    }


    @Override
    public HashMap getTopK() {
        return topK;
    }

    @Override
    public long getTotalCardinality() {
        return size;
    }


    protected static class CMTopKMergeException extends TopKMergeException {
        public CMTopKMergeException(String message) {
            super(message);
        }
    }

}
