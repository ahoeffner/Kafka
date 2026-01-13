package consumer;

public class Test
{
  public static void main(String[] args)
  {
    String group = "default";
    if (args.length == 1) group = args[0];
    Consumer consumer = new Consumer(group);
    consumer.consume();
  }
}
