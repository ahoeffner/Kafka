package client;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;


public class Producer implements Callback
{
  private Exception exception = null;
  private RecordMetadata metadata = null;
  private final KafkaProducer<String,String> producer;

  public Producer() throws Exception
  {
    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

    producer = new KafkaProducer<String,String>(props);
  }


  public synchronized RecordMetadata send(String topic, String key, String message) throws Exception
  {
    ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,message);
    producer.send(record,this);

    synchronized(this)
    {
      while(metadata == null && exception == null)
        this.wait();
    }

    if (exception != null)
      throw exception;

    return(metadata);
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception)
  {
    this.metadata = metadata;
    this.exception = exception;

    synchronized(this)
    {this.notifyAll();}
  }
}
