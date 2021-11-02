package setup;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSetup {
    private static Logger LOG = LoggerFactory.getLogger(StreamSetup.class);
    public static void main(String... args) throws Exception {
        Connection nc = Nats.connect("nats://localhost:4222");
        JetStreamManagement jsm = nc.jetStreamManagement();

        //Create quotes stream
        StreamConfiguration quotesStreamConfig = StreamConfiguration
                .builder()
                .name("QUOTES")
                .addSubjects("quotes.*")
                .discardPolicy(DiscardPolicy.Old)
                .maxMessages(1000000)
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build();

        jsm.addStream(quotesStreamConfig);

        //Create positions stream
        StreamConfiguration positionsStreamConfig = StreamConfiguration
                .builder()
                .name("POSITIONS")
                .addSubjects("positions")
                .discardPolicy(DiscardPolicy.Old)
                .maxMessages(100000)
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build();

        jsm.addStream(positionsStreamConfig);

        //Create market value stream
        StreamConfiguration marketValueStream = StreamConfiguration
                .builder()
                .name("MVSTR")
                .addSubjects("mvupdates")
                .discardPolicy(DiscardPolicy.Old)
                .maxMessages(100000)
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build();

        jsm.addStream(marketValueStream);

        nc.close();
    }
}
