package topics;


public class Main
{
  public static void main(String[] args) throws Exception
  {
    if (args.length < 2)
    {
      System.out.println("args: topic partitions");
      System.exit(-1);
    }

    String name = args[0];
    int partitions = Integer.parseInt(args[1]);

    Topic topic = new Topic();

    try {topic.delete(name);}
    catch (Exception e) {System.err.println(e.getMessage());}

    topic.create(name, partitions);

    System.out.println("Topic created: " + name + " with " + partitions + " partitions");
  }
}
