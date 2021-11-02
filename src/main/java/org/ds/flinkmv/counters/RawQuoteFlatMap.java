package org.ds.flinkmv.counters;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class RawQuoteFlatMap implements FlatMapFunction<Tuple2<String,String>, String> {
    private Counter counter = new Counter("{} quotes in {} ms - {} per second");

    @Override
    public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<String> collector) throws Exception {
        String countString = counter.count();
        if(countString != null) {
            collector.collect(countString);
        }
    }
}
