package producer;

import org.apache.kafka.clients.producer.*;


public class Test
{
  public static void main(String[] args) throws Exception
  {
    String msg = String.join(" ", args);

    Producer producer = new Producer();
    RecordMetadata metadata = producer.send("testtopic",null,msg);

    System.out.println("Message sent to topic: "+metadata.topic());
  }
}
