package org.ds.flinkmv.counters;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class KeyedRawQuoteCountFlatMap extends RichFlatMapFunction<Tuple2<String,String>, String> {
    private transient ValueState<Counter> counter;

    @Override
    public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<String> collector) throws Exception {
        Counter quoteCounter = counter.value();
        if(quoteCounter == null) {
            quoteCounter = new Counter("{} quotes in {} ms - {} per second");
            quoteCounter.count();
            counter.update(quoteCounter);
            return;
        }

        String countString = quoteCounter.count();
        if(countString != null) {
            collector.collect(countString);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Counter> descriptor =
                new ValueStateDescriptor<Counter>(
                        "Counter",
                        TypeInformation.of(
                                new TypeHint<Counter>() {
                                }
                        )
                );
        counter = getRuntimeContext().getState(descriptor);
    }
}
