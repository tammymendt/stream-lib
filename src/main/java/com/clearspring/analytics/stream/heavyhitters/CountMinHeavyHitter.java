package com.clearspring.analytics.stream.heavyhitters;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tamara on 2/18/2015.
 */
public class CountMinHeavyHitter implements IHeavyHitter {

    CountMinSketch countMinSketch;
    HashMap<Object,Long> heavyHitters;
    double phi;
    long size;

    public CountMinHeavyHitter(CountMinSketch countMinSketch, double phi){
        this.countMinSketch = countMinSketch;
        this.size = 0;
        this.phi = phi;
        this.heavyHitters = new HashMap<Object,Long>();
    }

    public void addLong(long item, long count) {
        countMinSketch.add(item,count);
        size+=count;
        updateHeavyHitters(item);
    }

    public void addString(String item, long count) {
        countMinSketch.add(item,count);
        size+=count;
        updateHeavyHitters(item);
    }

    @Override
    public void addObject(Object o) {
        long objectHash = MurmurHash.hash(o);
        countMinSketch.add(objectHash, 1);
        size+=1;
        updateHeavyHitters(objectHash);
    }

    private void updateHeavyHitters(long item){
        long minFrequency = (long)Math.ceil(size*phi);
        long estimateCount = countMinSketch.estimateCount(item);
        if (estimateCount >= minFrequency){
            heavyHitters.put(item, estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void updateHeavyHitters(String item){
        long minFrequency = (long)Math.ceil(size*phi);
        long estimateCount = countMinSketch.estimateCount(item);
        if (estimateCount >= minFrequency){
            heavyHitters.put(item, estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void removeNonFrequent(long minFrequency){
        ArrayList<Object> nonFrequentKeys = new ArrayList<Object>();
        for (Map.Entry<Object, Long> entry : heavyHitters.entrySet()){
            if(entry.getValue()<minFrequency){
                nonFrequentKeys.add(entry.getKey());
            }
        }
        for (Object o:nonFrequentKeys){
            heavyHitters.remove(o);
        }
    }

    public static CountMinHeavyHitter merge(CountMinHeavyHitter... estimators) throws CMHeavyHitterMergeException {
        CountMinSketch[] countMinSketches = new CountMinSketch[estimators.length];
        CountMinSketch mergedSketch;

        for (int i=0;i<estimators.length;i++){
            countMinSketches[i] = estimators[i].countMinSketch;
        }
        try {
            mergedSketch = CountMinSketch.merge(countMinSketches);
        }catch (Exception ex){
            throw new CMHeavyHitterMergeException("Cannot merge count min sketches: "+ex.getMessage());
        }

        double phi = estimators[0].phi;
        Map<Object,Long> topK = new HashMap<Object,Long>();
        CountMinHeavyHitter mergedTopK = new CountMinHeavyHitter(mergedSketch,phi);

        if (estimators != null && estimators.length > 0) {
            for (CountMinHeavyHitter estimator : estimators) {
                if (estimator.phi != phi) {
                    throw new CMHeavyHitterMergeException("Frequency expectation cannot be merged");
                }
                for (Map.Entry<Object, Long> entry : estimator.heavyHitters.entrySet()) {
                    if (topK.containsKey(entry.getKey())){
                        topK.put(entry.getKey(),mergedSketch.estimateCount((long)entry.getKey()));
                    }else{
                        topK.put(entry.getKey(),entry.getValue());
                    }
                }
            }
        }
        long minFrequency = (long) Math.ceil(mergedTopK.size * estimators[0].phi);
        mergedTopK.heavyHitters = new HashMap<Object,Long>(topK);
        mergedTopK.removeNonFrequent(minFrequency);
        return mergedTopK;
    }


    @Override
    public HashMap getHeavyHitters() {
        return heavyHitters;
    }

    @Override
    public long getTotalCardinality() {
        return size;
    }


    protected static class CMHeavyHitterMergeException extends HeavyHitterMergeException {
        public CMHeavyHitterMergeException(String message) {
            super(message);
        }
    }

}
