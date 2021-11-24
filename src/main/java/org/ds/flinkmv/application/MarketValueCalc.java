/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ds.flinkmv.application;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.ds.flinkmv.connectors.NatsStreamSink;
import org.ds.flinkmv.connectors.NatsStreamSource;
import org.ds.flinkmv.functions.*;
import org.ds.flinkmv.pojos.MarketValue;
import org.ds.flinkmv.pojos.Position;
import org.ds.flinkmv.pojos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MarketValueCalc {
	private static Logger LOG = LoggerFactory.getLogger(MarketValueCalc.class);
	private static final String NATS_URL = "nats://localhost:4222";

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NatsStreamSource nss = new NatsStreamSource(NATS_URL, "sc","quotes.>");
		DataStream<Tuple2<String,String>> rawQuoteStream = env.addSource(nss)
				.name("raw quote stream").uid("raw quote stream");


		DataStream<Quote> quoteStream = rawQuoteStream
				.filter(new QuoteStructureFilter()).uid("quote filter").name("quote filter")
				.map(new QuoteMapper()).uid("quote pojo mapper").name("quote pojo mapper");

		NatsStreamSource positionsSource = new NatsStreamSource(
				NATS_URL, "pc", "positions"
		);
		DataStream<Tuple2<String,String>> rawPositionsStream =
				env.addSource(positionsSource).uid("raw positions").name("raw positions");

		DataStream<Position> positions = rawPositionsStream
				.filter(new RawPositionFilterFunction()).uid("positions filter").name("positions filter")
				.map(new RawPositionsMapper()).uid("positions pojo mapper").name("positions pojo mapper");



		KeyedStream<Position, String> positionsByAccount =
				positions.keyBy((KeySelector<Position,String>) position -> position.owner);

		positionsByAccount.print().uid("positions by account printer").name("positions by account printer");

		MapStateDescriptor<Void,Quote> broadcastDescriptor =
				new MapStateDescriptor<Void, Quote>("quotes", Types.VOID,Types.POJO(Quote.class));

		BroadcastStream<Quote> quotes = quoteStream
				.keyBy(quote -> quote.symbol)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
				.reduce(new ReduceFunction<Quote>() {
					@Override
					public Quote reduce(Quote quote, Quote t1) throws Exception {
						return t1;
					}
				}).uid("quote reducer").name("quote reducer")
				.broadcast(broadcastDescriptor);

		//Connect the streams
		DataStream<MarketValue> balanceCalcStream =
				positionsByAccount
						.connect(quotes)
						.process(new QuoteEvaluator()).uid("mv calc stream").name("mv calc stream");

		//balanceCalcStream.print();
		balanceCalcStream
				.map(new MarketValueMapper()).uid("mv calc output mapper").name("mv calc output mapper")
				.addSink(new NatsStreamSink(NATS_URL,"mvupdates"));

		env.execute("Flink Streaming Java API Skeleton");
	}
}

//TODO: Is a consumer group needed for the durable subscription?