package com.clearspring.analytics.stream.frequency;

public interface IFrequency {

    void add(long item, long count);

    void add(String item, long count);
    
    void delete(String item, long count);

    long estimateCount(long item);

    long estimateCount(String item);

    long size();
}
