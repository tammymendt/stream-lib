package com.clearspring.analytics.stream.frequency;

import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Created by Tamara on 2/18/2015.
 */
public class CountMinTopKTest {

    @Test
    public void testAccuracy() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        double phi = 0.4;
        int frequency = (int)Math.ceil(numItems*phi);

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

        int[] actualFreq = new int[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        Map<Object, Long> topK = sketch.getTopK();
/*
        System.out.println("Printing the content of TopK Map");
        for (Map.Entry<Object,Long> entry : topK.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
*/
        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i]>=frequency){
                assertTrue("Frequent item not found: item " + i + ", frequency " + actualFreq[i], !topK.containsKey(i));
            }
        }
    }

}
