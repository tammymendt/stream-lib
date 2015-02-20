package com.clearspring.analytics.stream.frequency;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tamara on 2/18/2015.
 */
public class CountMinTopK extends CountMinSketch{

    Map<Object,Long> topK;
    double phi;

    public CountMinTopK(int depth, int width, int seed, double phi){
        super(depth, width, seed);
        this.phi = phi;
        this.topK = new HashMap<Object,Long>();
    }

    public CountMinTopK(double epsOfTotalCount, double confidence, int seed, double phi){
        super(epsOfTotalCount,confidence,seed);
        this.phi = phi;
        this.topK = new HashMap<Object,Long>();
    }

    public CountMinTopK(int depth, int width, int size, long[] hashA, long[][] table, double phi){
        super(depth,width,size,hashA,table);
        this.phi = phi;
        this.topK = new HashMap<Object,Long>();
    }

    public CountMinTopK(CountMinSketch countMinSketch, double phi){

        this.depth = countMinSketch.depth;
        this.width = countMinSketch.width;
        this.eps = countMinSketch.eps;
        this.confidence = countMinSketch.confidence;
        this.size = countMinSketch.size;
        this.hashA = Arrays.copyOf(countMinSketch.hashA, countMinSketch.hashA.length);

        this.table = new long[depth][width];
        for (int i = 0; i < countMinSketch.table.length; i++) {
            for (int j = 0; j < countMinSketch.table[i].length; j++) {
                this.table[i][j] += countMinSketch.table[i][j];
            }
        }

        this.phi = phi;
        this.topK = new HashMap<Object,Long>();
    }

    @Override
    public void add(long item, long count) {
        super.add(item,count);
        updateTopK(item);
    }

    @Override
    public void add(String item, long count) {
        super.add(item,count);
        updateTopK(item);
    }

    private void updateTopK(long item){
        long minFrequency = (long)Math.ceil(size*phi);
        long estimateCount = estimateCount(item);
        if (estimateCount >= minFrequency){
            topK.put(item,estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void updateTopK(String item){
        long minFrequency = (long)Math.ceil(size*phi);
        long estimateCount = estimateCount(item);
        if (estimateCount >= minFrequency){
            topK.put(item,estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void removeNonFrequent(long minFrequency){
        for (Map.Entry<Object, Long> entry : topK.entrySet()){
            if(entry.getValue()<minFrequency){
                topK.remove(entry);
            }
        }
    }

    public static CountMinTopK merge(CountMinTopK... estimators) throws CMSMergeException {
        CountMinSketch mergedSketch = (CountMinSketch) CountMinSketch.merge(estimators);
        double phi = estimators[0].phi;
        Map<Object,Long> topK = new HashMap<Object,Long>();
        CountMinTopK merged = new CountMinTopK(mergedSketch,phi);

        if (estimators != null && estimators.length > 0) {
            for (CountMinTopK estimator : estimators) {
                if (estimator.phi != phi) {
                    throw new CMSMergeException("Cannot merge estimators of frequency expectation");
                }
                for (Map.Entry<Object, Long> entry : estimator.topK.entrySet()) {
                    if (topK.containsKey(entry.getKey())){
                        topK.replace(entry.getKey(),mergedSketch.estimateCount((long)entry.getKey()));
                        //long oldValue = topK.get(entry.getKey());
                        //topK.replace(entry.getKey(),oldValue+entry.getValue());
                    }else{
                        topK.put(entry.getKey(),entry.getValue());
                    }
                }
            }
        }
        long minFrequency = (long) Math.ceil(merged.size * estimators[0].phi);
        merged.topK = new HashMap<Object,Long>(topK);
        merged.removeNonFrequent(minFrequency);
        return merged;
    }
}
