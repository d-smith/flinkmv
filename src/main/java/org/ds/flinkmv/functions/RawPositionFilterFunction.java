package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class RawPositionFilterFunction implements FilterFunction<Tuple2<String,String>> {

    @Override
    public boolean filter(Tuple2<String,String> t2) throws Exception {

        try {
            String[] tokens = t2.f1.split(",");
            Double.valueOf(tokens[2]);
            return tokens.length == 3;
        } catch(Throwable t) {
            return false;
        }
    }
}
