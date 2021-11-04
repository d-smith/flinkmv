package org.ds.flinkmv.functions;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.ds.flinkmv.pojos.MarketValue;
import org.ds.flinkmv.pojos.Position;
import org.ds.flinkmv.pojos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class QuoteEvaluator extends KeyedBroadcastProcessFunction<String, Position, Quote, MarketValue> {

    private static final Logger LOG = LoggerFactory.getLogger(QuoteEvaluator.class);
    // broadcast state descriptor
    MapStateDescriptor<Void, Quote> quoteDesc;

    MapState<String, Map<String,Position>> positionsState;



    @Override
    public void open(Configuration conf) {
        quoteDesc = new MapStateDescriptor<>("quotes", Types.VOID, Types.POJO(Quote.class));

        MapStateDescriptor<String,Map<String,Position>> positionsStateDesc =
                new MapStateDescriptor<>("position", Types.STRING,
                        TypeInformation.of(
                                new TypeHint<Map<String,Position>>() {
                                }
                        )
                );

        positionsState = getRuntimeContext().getMapState(positionsStateDesc);
    }

    @Override
    public void processElement(Position position, ReadOnlyContext readOnlyContext, Collector<MarketValue> collector) throws Exception {
        System.out.println("processElement " + position);
        LOG.warn("you seen my cones");

        // get current quote from broadcast state
        Quote quote = readOnlyContext
                .getBroadcastState(this.quoteDesc)
                .get(null);
        System.out.println(quote);

        if(position != null && quote != null && position.symbol.equals(quote.symbol)) {
            collector.collect(new MarketValue(position.owner, position.symbol,position.amount, position.amount * quote.price));
        }

        //Store it

        if(!positionsState.isEmpty()) {
            Map<String,Position> positionsWithSymbol = positionsState.get(position.symbol);
            if(positionsWithSymbol == null) {
                Map<String,Position> m = new HashMap<>();
                m.put(position.owner,position);
                positionsState.put(position.symbol, m);
            } else {
                positionsWithSymbol.put(position.owner, position);
            }
        } else {
            Map<String,Position> m = new HashMap<>();
            m.put(position.owner,position);
            positionsState.put(position.symbol,m);
        }

    }

    @Override
    public void processBroadcastElement(Quote quote, Context context, Collector<MarketValue> collector) throws Exception {
        // store the new quote by updating the broadcast state
        BroadcastState<Void, Quote> bcState = context.getBroadcastState(quoteDesc);
        bcState.put(null, quote);

        //Go through the positions associated the current symbol...

        MapStateDescriptor<String,Map<String,Position>> positionsStateDesc =
                new MapStateDescriptor<>("position", Types.STRING,
                        TypeInformation.of(
                                new TypeHint<Map<String,Position>>() {
                                }
                        )
                );

        context.applyToKeyedState(positionsStateDesc, new KeyedStateFunction<String, MapState<String, Map<String, Position>>>() {
            @Override
            public void process(String s, MapState<String, Map<String, Position>> stringMapMapState) throws Exception {

                Map<String,Position> positionsWithSymbols =  stringMapMapState.get(quote.symbol);
                if(positionsWithSymbols != null) {
                    positionsWithSymbols.values().forEach(position -> collector.collect(
                            new MarketValue(position.owner, position.symbol, position.amount, position.amount * quote.price)));
                }

            }
        });

    }
}

