package topics;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Collections;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AdminClient;


public class Topic
{
    private static final int retention = 24*60*60000; // 1 hour
    private static final short replication = 1; // single broker


    public void create(String name, int partitions) throws Exception
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        AdminClient admin = AdminClient.create(props);
        NewTopic topic = new NewTopic(name, partitions, replication);

        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", String.valueOf(retention));

        admin.createTopics(Collections.singleton(topic)).all().get();
    }

    public void delete(String name) throws Exception
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        AdminClient admin = AdminClient.create(props);
        admin.deleteTopics(Collections.singletonList(name)).all().get();

        Thread.sleep(2000);
    }
}
