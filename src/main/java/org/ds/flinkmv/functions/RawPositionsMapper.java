package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.ds.flinkmv.application.Position;

public class RawPositionsMapper implements MapFunction<String, Position> {

    @Override
    public Position map(String s) throws Exception {
        String[] tokens = s.split(",");
        return new Position(tokens[0], tokens[1], Double.valueOf(tokens[2]));
    }
}
