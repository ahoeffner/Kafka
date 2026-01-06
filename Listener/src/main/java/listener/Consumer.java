package listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer
{
  public final int poll = 2;
  private boolean running = true;
  private boolean stopped = false;
  private final KafkaConsumer<String,String> consumer;

  public Consumer(String group)
  {
    Collection<String> topics = new ArrayList<String>();
    topics.add("testtopic");

    System.out.println("Listening with group id: "+group);

    Properties props = new Properties();
    props.put("group.id",group);
    props.put(ConsumerConfig.GROUP_ID_CONFIG,group);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

    consumer = new KafkaConsumer<String,String>(props);
    consumer.subscribe(topics);

    ShutdownHook hook = new ShutdownHook(this);
    Runtime.getRuntime().addShutdownHook(hook);
  }


  public void consume()
  {
    System.out.println("\n\nListening for messages\n\n");

    while(running)
    {
      ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(poll));
      for(ConsumerRecord<String,String> record : records) System.out.println(record.topic()+" received: "+record.key()+" "+record.value());
    }

    consumer.close();
    System.out.println("Disconnected");

    stopped = true;
  }


  private void disconnect()
  {
    System.out.println("\n\nShutdown");
    running = false;

    while(!stopped)
    {
      try {Thread.sleep(100);}
      catch (Exception e) {;}
    }
  }


  class ShutdownHook extends Thread
  {
    private final Consumer consumer;

    private ShutdownHook(Consumer consumer)
    {
      this.consumer = consumer;
    }

    @Override
    public void run()
    {
      consumer.disconnect();
    }
  }
}
