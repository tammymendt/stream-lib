package com.clearspring.analytics.stream.frequency;

public interface IFrequency {

    void add(long item, long count);

    void add(String item, long count);

    void add(Object item, long count);

    long estimateCount(long item);

    long estimateCount(String item);

    long estimateCount(Object item);

    long size();
}
