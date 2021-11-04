package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.ds.flinkmv.pojos.Quote;

public class QuoteMapper implements MapFunction<Tuple2<String,String>, Quote> {

    @Override
    public Quote map(Tuple2<String,String> t2) throws Exception {
        String[] subjectParts = t2.f0.split("\\.");
        return new Quote(subjectParts[1], Double.valueOf(t2.f1));
    }
}
