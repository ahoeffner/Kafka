package client;

import org.apache.kafka.clients.producer.*;


public class Test
{
  public static void main(String[] args) throws Exception
  {
    if (args.length < 2)
    {
      System.out.println("args: key value");
      System.exit(-1);
    }

    String msg = args[1];

    for (int i = 2; i < args.length; i++)
      msg += " " + args[i];

    Producer producer = new Producer();
    RecordMetadata metadata = producer.send("testtopic",args[0],msg);

    System.out.println("Message sent to topic: "+metadata.topic());
  }
}
