package com.clearspring.analytics.stream.frequency;

import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by Tamara on 2/18/2015.
 */
public class CountMinTopKTest {

    @Test
    public void testAccuracy() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        double phi = 0.2;
        long frequency = (int)Math.ceil(numItems*phi);

        int[] xs = new int[numItems];
        int maxScale = 20;

        for (int i = 0; i < numItems/2; i++) {
            xs[i] = 1;
        }

        for (int i = 0; i < numItems/2; i++) {
            int scale = r.nextInt(maxScale);
            xs[numItems/2 + i] = r.nextInt(1 << scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinTopK sketch = new CountMinTopK(epsOfTotalCount, confidence, seed, phi);
        for (int x : xs) {
            sketch.add(x, 1);
        }

        long[] actualFreq = new long[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        System.out.println("Printing the content of TopK Map");
        for (Map.Entry<Object,Long> entry : sketch.topK.entrySet()){
            System.out.println(entry.getKey()+" ("+entry.getKey().getClass()+"): "+entry.getValue()+"\n");
        }

        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i]>=frequency){
                assertTrue("Frequent item not found: item " + i + ", frequency " + actualFreq[i], sketch.topK.containsKey(i));
            }else{
                assertTrue("False Positive: " + i + ", frequency " + actualFreq[i] + " (min expected frequency "+frequency+")", !sketch.topK.containsKey(i));
            }
        }
    }

    @Test
    public void merge() throws CountMinSketch.CMSMergeException {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;
        double phi = 0.2;

        int maxScale = 20;
        Random r = new Random();

        CountMinTopK baseline = new CountMinTopK(epsOfTotalCount, confidence, seed, phi);
        CountMinTopK[] sketches = new CountMinTopK[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketches[i] = new CountMinTopK(epsOfTotalCount, confidence, seed, phi);
            for (int j = 0; j < cardinality; j++) {
                if (r.nextDouble()<0.2){
                    sketches[i].add(1, 1);
                    baseline.add(1, 1);
                    sketches[i].add(50, 1);
                    baseline.add(50, 1);
                }
                int scale = r.nextInt(maxScale);
                int val = r.nextInt(1 << scale);
                sketches[i].add(val, 1);
                baseline.add(val, 1);
            }
        }

        CountMinTopK merged = CountMinTopK.merge(sketches);

        for (Map.Entry<Object, Long> entry : baseline.topK.entrySet()){
            System.out.println("\nFrequent in Baseline: "+entry.getKey()+": "+entry.getValue());
            assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.topK.containsKey(entry.getKey()));
            System.out.println("\nFrequent Detected: "+entry.getKey()+": "+ merged.topK.get(entry.getKey()));
            //assertEquals(entry.getValue(), merged.topK.get(entry.getKey()));
        }
    }

}
