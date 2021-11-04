package org.ds.flinkmv.counters;

public class PublishCounter extends Counter {
    public PublishCounter() {
        super("{} published in {} ms - {} per second");
    }
}
