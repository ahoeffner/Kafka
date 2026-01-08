package topics;

import java.util.Properties;
import java.util.Collections;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AdminClient;


public class Topic
{
    public void create(String name, int partitions) throws Exception
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        AdminClient admin = AdminClient.create(props);
        NewTopic topic = new NewTopic(name, partitions, (short) 1);
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
