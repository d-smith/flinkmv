package org.ds.flinkmv.connectors;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public class NatsStreamSource extends RichSourceFunction<Tuple2<String,String>> {

    private static Logger LOG = LoggerFactory.getLogger(NatsStreamSource.class);
    private String natsUrl;
    private boolean running = true;
    private String consumerName;
    private String subject;

    public NatsStreamSource(String natsUrl, String consumerName, String subject)  {
       this.natsUrl = natsUrl;
       this.consumerName = consumerName;
       this.subject = subject;
    }

    private Connection getConnection() throws Exception {
        return Nats.connect(natsUrl);
    }

    private JetStreamSubscription createSubscription(Connection nc) throws Exception {

        JetStream js = nc.jetStream();

        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable(consumerName)
                .configuration(ConsumerConfiguration.builder()
                        .deliverPolicy(DeliverPolicy.New)
                        .build())
                .build();

        return js.subscribe(subject, pullOptions);
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        LOG.info("open nats connection");
        Connection nc = getConnection();

        LOG.info("create subscription");
        JetStreamSubscription subscription = createSubscription(nc);

        LOG.info("pull events");
        while(running) {
            List<Message> messages = subscription.fetch(250, Duration.ofMillis(100));
            for(Message m: messages) {
                sourceContext.collect(
                        Tuple2.of(m.getSubject(), new String(m.getData(), StandardCharsets.UTF_8))
                );
                m.ack();
            }
        }

        //No longer running
        LOG.info("close nats connection");
        nc.close();
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
