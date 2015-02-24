package com.clearspring.analytics.stream.topK;

import java.util.HashMap;

/**
 * Created by tamara on 23.02.15.
 */
public interface TopK {

    void addObject(Object o);

    HashMap getTopK();

    long getTotalCardinality();
}
