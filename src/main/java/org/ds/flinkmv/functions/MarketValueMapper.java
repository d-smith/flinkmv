package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.ds.flinkmv.application.MarketValue;

public class MarketValueMapper implements MapFunction<MarketValue,String> {
    @Override
    public String map(MarketValue marketValue) throws Exception {
        return marketValue.toString();
    }
}
