package producers;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import org.ds.flinkmv.counters.CompletionCounter;
import org.ds.flinkmv.counters.PublishCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class QuotesProducer {
    static Logger LOG = LoggerFactory.getLogger(QuotesProducer.class);

    public static void main(String... args) throws Exception {

        String[] subjects =  {
                "quotes.DWAC",
                "quotes.SNDL",
                "quotes.CEI",
                "quotes.FAMI",
                "quotes.KMDN"
        };

        LOG.info("Connect to nats...");
        Connection nc = Nats.connect("nats://localhost:4222");
        JetStream js = nc.jetStream();
        LOG.info("Jetstream!");

        int count = 0;
        long startTime = System.currentTimeMillis();
        CompletionCounter completionCounter = new CompletionCounter();
        PublishCounter pc = new PublishCounter();

        final int roundSize = 250; //publish this many before checking acks
        int roundCount = 0;
        List<CompletableFuture<PublishAck>> futures = new ArrayList<>();

        for(;;) {

            int idx = (int) (Math.random() * subjects.length);
            String subject = subjects[idx];
            byte[] randoPrice = String.valueOf((Math.random() * 600)).getBytes(StandardCharsets.UTF_8);

            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(randoPrice)
                    .build();

            pc.count();
            futures.add(js.publishAsync(msg));
            roundCount++;

            if(roundCount == roundSize) {
                LOG.debug("check acks");
                roundCount = 0;

                while (futures.size() > 0) {
                    List<CompletableFuture<PublishAck>> notDone = new ArrayList<>((int)roundSize);
                    for (CompletableFuture<PublishAck> f : futures) {
                        if (!f.isDone()) {
                            notDone.add(f);
                        }
                        else if (f.isCompletedExceptionally()) {
                            // handle the exception, either an io error or expectation error
                        }
                        else {
                            // handle the completed ack
                            PublishAck pa = f.get(); // this call blocks if it's not done, but we know it's done because we checked
                        }
                    }
                    futures = notDone;
                }
                LOG.debug("check of acks complete");

                Thread.sleep(15);
            }
        }
    }
}
