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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.ds.flinkmv.connectors.NatsStreamSource;
import org.ds.flinkmv.counters.RawQuoteFlatMap;
import org.ds.flinkmv.functions.QuoteMapper;
import org.ds.flinkmv.functions.QuoteStructureFilter;
import org.ds.flinkmv.functions.RawPositionFilterFunction;
import org.ds.flinkmv.functions.RawPositionsMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamingJob {
	private static Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	public static void main(String[] args) throws Exception {
		LOG.info("ALIVE!!!!");
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NatsStreamSource nss = new NatsStreamSource("nats://localhost:4222", "sc","quotes.>");
		DataStream<Tuple2<String,String>> rawQuoteStream = env.addSource(nss);


		rawQuoteStream
				.filter(new QuoteStructureFilter())
				.map(new QuoteMapper())
				.print();

		NatsStreamSource positionsSource = new NatsStreamSource(
				"nats://localhost:4222", "pc", "positions"
		);
		DataStream<Tuple2<String,String>> rawPositionsStream = env.addSource(positionsSource);

		rawPositionsStream
				.filter(new RawPositionFilterFunction())
				.map(new RawPositionsMapper())
				.print();

		env.execute("Flink Streaming Java API Skeleton");
	}
}

//TODO: Is a consumer group needed for the durable subscription?