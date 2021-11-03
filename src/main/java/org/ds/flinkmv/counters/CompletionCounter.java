package org.ds.flinkmv.counters;

import io.nats.client.api.PublishAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionCounter {
    private AtomicInteger count;
    private AtomicInteger errorCount;
    private long epoch;
    private AtomicBoolean firstCall;

    private Logger LOG = LoggerFactory.getLogger(CompletionCounter.class);

    public CompletionCounter() {
        count = new AtomicInteger();
        errorCount = new AtomicInteger();
        epoch = -1;
        firstCall = new AtomicBoolean(false);
    }

    public void count(PublishAck publishAck, Throwable throwable) {
        if(firstCall.getAndSet(true) == false) {
            epoch = System.currentTimeMillis();
        }
        if(publishAck != null) {
            int current = count.incrementAndGet();
            if(current % 10000 == 0) {
                long now = System.currentTimeMillis();
                LOG.info("{} publish futures completed is {} ms - {} per second", current, now - epoch, (1000.0 * current)/(now - epoch));
                count.set(0);
                epoch = System.currentTimeMillis();
            }
        } else if (throwable != null) {
            int errors = errorCount.incrementAndGet();
            LOG.info("error {} total errors {}", throwable.getMessage(), errors);
        }
    }
}
