package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class QuoteStructureFilter implements FilterFunction<Tuple2<String,String>> {

    @Override
    public boolean filter(Tuple2<String, String> t2) throws Exception {
        try {
            String[] subjectParts = t2.f0.split("\\.");
            if(subjectParts.length != 2) {
                return false;
            }

            Double.valueOf(t2.f1);
            return true;
        } catch(Throwable t) {
            return false;
        }
    }
}
