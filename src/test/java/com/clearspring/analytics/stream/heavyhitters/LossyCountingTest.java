package com.clearspring.analytics.stream.heavyhitters;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Random;

/**
 * Created by tamara on 08.04.15.
 */
public class LossyCountingTest {

    @Test
    public void testAccuracy() {
        int seed = 734181;
        Random r = new Random(seed);
        int numItems = 10000;
        double phi = 0.05;
        long frequency = (int)Math.ceil(numItems*phi);

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

        double error = 0.1;
        double confidence = 0.05;

        LossyCounting lossyCounting = new LossyCounting(phi,error,confidence);

        for (int x : xs) {
            lossyCounting.addObject(x);
        }

        long[] actualFreq = new long[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        for (Map.Entry<Object, String> entry : lossyCounting.getHeavyHitters().entrySet()){
            System.out.println("Heavy Hitter: "+entry.getKey()+": "+entry.getValue());
        }

        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i]>=frequency){
                assertTrue("Frequent item not found: item " + i + ", frequency " + actualFreq[i], lossyCounting.heavyHitters.containsKey(i));
            }else{
                assertTrue("False Positive: " + i + ", frequency " + actualFreq[i] + " (min expected frequency "+frequency+")", !lossyCounting.heavyHitters.containsKey(i));
            }
        }
    }


}
