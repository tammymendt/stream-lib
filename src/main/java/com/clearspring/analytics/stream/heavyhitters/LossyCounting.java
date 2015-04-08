package com.clearspring.analytics.stream.heavyhitters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tamara on 08.04.15.
 */
public class LossyCounting implements IHeavyHitter{

    final int PERIOD = 100;

    double fraction;
    double error;
    double confidence;
    int cardinality;
    Map<Object,Counter> heavyHitters;
    int delta;

    private class Counter{
        long lowerBound;
        long counterDelta;

        private Counter(long lowerBound, long counterDelta){
            this.lowerBound = lowerBound;
            this.counterDelta = counterDelta;
        }

        private void updateLowerBound(int count){
            lowerBound+=count;
        }

        private long getUpperBound(){
            return lowerBound+ counterDelta;
        }

    }

    public LossyCounting(double fraction, double error, double confidence){

        this.fraction = fraction;
        this.error = error;
        this.confidence = confidence;
        this.cardinality = 0;
        this.heavyHitters = new HashMap<Object, Counter>();
        this.delta = 0;
    }

    @Override
    public void addObject(Object o) {
        if (heavyHitters.containsKey(o)){
            heavyHitters.get(o).updateLowerBound(1);
        }else{
            heavyHitters.put(o,new Counter(1,delta));
        }
        cardinality+=1;
        if ((int)Math.floor(cardinality*fraction)!=delta){
            delta+=1;
            //decreaseLowerBound();
            updateHeavyHitters();
        }
    }

    public void decreaseLowerBound(){
        for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()) {
            entry.getValue().updateLowerBound(-1);
        }
    }

    public void updateHeavyHitters(){
        ArrayList<Object> nonFrequentKeys = new ArrayList<Object>();
        for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
            if (entry.getValue().getUpperBound()<delta || entry.getValue().lowerBound==0){
                nonFrequentKeys.add(entry.getKey());
            }
        }
        for (Object o:nonFrequentKeys){
            heavyHitters.remove(o);
        }
    }

    public void merge(LossyCounting toMerge) throws HeavyHitterMergeException {
        if (this.error!=toMerge.error || this.confidence!=toMerge.confidence || this.fraction!=toMerge.fraction){
            throw new HeavyHitterMergeException("Both heavy hitter structures must be identical");
        }
        this.cardinality+=toMerge.cardinality;
        for (Map.Entry<Object, Counter> entry : toMerge.heavyHitters.entrySet()){
            Counter counter = this.heavyHitters.get(entry.getKey());
            if (counter==null){
                this.heavyHitters.put(entry.getKey(),entry.getValue());
            }else{
                Counter mergingCounter = entry.getValue();
                //todo can deltas be merged like this?
                this.heavyHitters.put(entry.getKey(),
                        new Counter(mergingCounter.lowerBound+counter.lowerBound,mergingCounter.counterDelta +counter.counterDelta));
            }
        }
        updateHeavyHitters();
    }

    @Override
    public HashMap<Object,String> getHeavyHitters() {
        HashMap<Object,String> heavyHitterCounters = new HashMap<>();
        for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
            heavyHitterCounters.put(entry.getKey(), String.valueOf(entry.getValue().lowerBound)+","+String.valueOf(entry.getValue().counterDelta));
        }
        return heavyHitterCounters;
    }

    @Override
    public long getTotalCardinality() {
        return cardinality;
    }
}
