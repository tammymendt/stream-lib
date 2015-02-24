package com.clearspring.analytics.stream.topK;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.topK.CountMinTopK;
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

        for (int i = 0; i < numItems; i++) {
            double p = r.nextDouble();
            if (p<0.2){
                xs[i] = 1;
            }else if (p<0.4) {
                xs[i] = 12;
            }else {
                int scale = r.nextInt(maxScale);
                xs[i] = r.nextInt(1 << scale);
            }
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinTopK cmTopK = new CountMinTopK(sketch,phi);

        for (int x : xs) {
            cmTopK.addLong(x, 1);
        }

        long[] actualFreq = new long[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i]>=frequency){
                assertTrue("Frequent item not found: item " + i + ", frequency " + actualFreq[i], cmTopK.topK.containsKey((long)i));
            }else{
                assertTrue("False Positive: " + i + ", frequency " + actualFreq[i] + " (min expected frequency "+frequency+")", !cmTopK.topK.containsKey((long)i));
            }
        }
    }

    @Test
    public void merge() throws CountMinTopK.CMTopKMergeException {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.0001;

        double confidence = 0.99;
        int seed = 7364181;
        double phi = 0.2;

        int maxScale = 20;
        Random r = new Random();

        CountMinSketch sketchBaseline = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinTopK baseline = new CountMinTopK(sketchBaseline,phi);

        CountMinTopK[] sketches = new CountMinTopK[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            CountMinSketch cms = new CountMinSketch(epsOfTotalCount, confidence, seed);
            sketches[i] = new CountMinTopK(cms, phi);
            for (int j = 0; j < cardinality; j++) {
                double p = r.nextDouble();
                if (p<0.2){
                    sketches[i].addLong(1 * i + 1, 1);
                    baseline.addLong(1 * i + 1, 1);
                }else if (p<0.4) {
                    sketches[i].addLong(50 * i + 1, 1);
                    baseline.addLong(50 * i + 1, 1);
                }else {
                    int scale = r.nextInt(maxScale);
                    int val = r.nextInt(1 << scale);
                    sketches[i].addLong(val, 1);
                    baseline.addLong(val, 1);
                }
            }
        }

        CountMinTopK merged = CountMinTopK.merge(sketches);

        for (Map.Entry<Object, Long> entry : baseline.topK.entrySet()){
            assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.topK.containsKey(entry.getKey()));
            assertEquals(entry.getValue(), merged.topK.get(entry.getKey()));
        }
    }

}
