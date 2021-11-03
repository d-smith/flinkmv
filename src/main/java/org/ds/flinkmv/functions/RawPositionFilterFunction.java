package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.FilterFunction;

public class RawPositionFilterFunction implements FilterFunction<String> {

    @Override
    public boolean filter(String s) throws Exception {
        try {
            String[] tokens = s.split(",");
            Double.valueOf(tokens[2]);
            return tokens.length == 3;
        } catch(Throwable t) {
            return false;
        }
    }
}
