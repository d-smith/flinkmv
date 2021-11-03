package org.ds.flinkmv.connectors;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.ds.flinkmv.counters.CompletionCounter;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class NatsStreamSink extends RichSinkFunction<String> {

    private String natsUrl;
    private String subject;
    private JetStream jetStream;
    private CompletionCounter counter;

    public NatsStreamSink(String natsUrl, String subject) {
        this.natsUrl = natsUrl;
        this.subject = subject;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection nc = Nats.connect(natsUrl);
        jetStream = nc.jetStream();
        counter = new CompletionCounter();
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        Message msg = NatsMessage.builder()
                .subject(this.subject)
                .data(value.getBytes(StandardCharsets.UTF_8))
                .build();

        CompletableFuture<PublishAck> f = jetStream.publishAsync(msg);
        f.whenComplete((ack,t)-> {
           counter.count(ack,t);
        });
    }
}
