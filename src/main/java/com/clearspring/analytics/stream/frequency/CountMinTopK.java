package com.clearspring.analytics.stream.frequency;

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
        int minFrequency = (int)Math.ceil(size*phi);
        long estimateCount = estimateCount(item);
        if (estimateCount >= minFrequency){
            topK.put(item,estimateCount);
        }
        for (Map.Entry<Object, Long> entry : topK.entrySet()){
            if(entry.getValue()<minFrequency){
                topK.remove(entry);
            }
        }
    }

    private void updateTopK(String item){
        int minFrequency = (int)Math.ceil(size*phi);
        long estimateCount = estimateCount(item);
        if (estimateCount >= minFrequency){
            topK.put(item,estimateCount);
        }
        for (Map.Entry<Object, Long> entry : topK.entrySet()){
            if(entry.getValue()<minFrequency){
                topK.remove(entry);
            }
        }
    }

    public Map<Object,Long> getTopK(){
        return topK;
    }
}
