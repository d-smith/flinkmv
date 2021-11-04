package org.ds.flinkmv.counters;

public class ConsumerCounter extends Counter {
    public ConsumerCounter() {
       super("{} consumed in {} ms - {} per second");
    }
}
