package com.clearspring.analytics.stream.heavyhitters;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Random;

/**
 * Created by tamara on 08.04.15.
 */
public class LossyCountingTest {

    static final double fraction = 0.05;
    static final double error = 0.01;
    static final double confidence = 0.05;
    static final int seed = 7364181;
    static final Random r = new Random(seed);

    @Test
    public void testAccuracy() {

        int numItems = 1000000;
        long frequency = (int)Math.ceil(numItems* fraction);

        int[] xs = new int[numItems];
        int maxScale = 20;

        for (int i = 0; i < numItems; i++) {
            double p = r.nextDouble();
            if (p<0.2){
                xs[i] = r.nextInt(5);
            }else {
                int scale = r.nextInt(maxScale);
                xs[i] = r.nextInt(1 << scale);
            }
        }

        LossyCounting lossyCounting = new LossyCounting(fraction,error,confidence);

        for (int x : xs) {
            lossyCounting.addObject(x);
        }

        long[] actualFreq = new long[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        /*for (Map.Entry<Object, String> entry : lossyCounting.getHeavyHitters().entrySet()){
            System.out.println("Heavy Hitter: "+entry.getKey()+": "+entry.getValue());
        }*/

        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i]>=frequency){
                assertTrue("Frequent item not found: item " + i + ", frequency " + actualFreq[i], lossyCounting.heavyHitters.containsKey(i));
            }else{
                assertTrue("False Positive: " + i + ", frequency " + actualFreq[i] + " (min expected frequency "+frequency+")", !lossyCounting.heavyHitters.containsKey(i));
            }
        }
    }

    @Test
    public void merge() throws HeavyHitterMergeException {
        int numToMerge = 5;
        int cardinality = 10000;
        int maxScale = 20;

        LossyCounting baseline = new LossyCounting(fraction,error,confidence);

        LossyCounting merged = new LossyCounting(fraction,error,confidence);

        LossyCounting[] sketches = new LossyCounting[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketches[i] = new LossyCounting(fraction,error/2,confidence);
            for (int j = 0; j < cardinality; j++) {
                double p = r.nextDouble();
                if (p<0.2){
                    int val = r.nextInt(5);
                    sketches[i].addObject(val);
                    baseline.addObject(val);
                }else {
                    int scale = r.nextInt(maxScale);
                    int val = r.nextInt(1 << scale);
                    sketches[i].addObject(val);
                    baseline.addObject(val);
                }
            }
            merged.merge(sketches[i]);
            //System.out.println("\nLossy Count: "+i);
            //System.out.println(sketches[i].toString());
        }

/*        System.out.println("\nMERGED\n");
        System.out.println(merged.toString());

        System.out.println("\nBASELINE\n");
        System.out.println(baseline.toString());
*/
        for (Map.Entry<Object, String> entry : baseline.getHeavyHitters().entrySet()){
            assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.heavyHitters.containsKey(entry.getKey()));
        }
    }

}
