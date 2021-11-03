package org.ds.flinkmv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.ds.flinkmv.application.Position;

public class RawPositionsMapper implements MapFunction<Tuple2<String,String>, Position> {

    @Override
    public Position map(Tuple2<String,String> t2) throws Exception {
        String[] tokens = t2.f1.split(",");
        return new Position(tokens[0], tokens[1], Double.valueOf(tokens[2]));
    }
}
