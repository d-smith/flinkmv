package org.ds.flinkmv.application;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.ds.flinkmv.connectors.NatsStreamSource;
import org.ds.flinkmv.functions.QuoteMapper;
import org.ds.flinkmv.functions.QuoteStructureFilter;
import org.ds.flinkmv.pojos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuoteConflator {
    Logger LOG = LoggerFactory.getLogger(QuoteConflator.class);
    private static final String NATS_URL = "nats://localhost:4222";

    public static void main(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        NatsStreamSource nss = new NatsStreamSource(NATS_URL, "sc","quotes.>");
        DataStream<Tuple2<String,String>> rawQuoteStream = env.addSource(nss);


        DataStream<Quote> quoteStream = rawQuoteStream
                .filter(new QuoteStructureFilter())
                .map(new QuoteMapper());


        quoteStream.keyBy(quote -> quote.symbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Quote>() {
                    @Override
                    public Quote reduce(Quote quote, Quote t1) throws Exception {
                        return t1;
                    }
                })
                .map(new MapFunction<Quote, String>() {
                    @Override
                    public String map(Quote quote) throws Exception {
                        return "windowed -> " + quote.toString();
                    }
                })
                .print();
        env.execute();
    }
}
