package utils;

public class Config {

    public static final String ROOT = "dataset/";
    public static final String filename = ROOT + "post.csv";
    public static final String delimiter = ",";
    public static final String TOPIC = "test";
    public static final Long START =1514764800000L ;
    public static final Long H24 =86400000L ;

    public static String kafkaBrokerList = "localhost:9092";
    public static String consumerGroup = "consumer";
    public static String OutTOPIC = "out";
    public static double wa = 0.3; // peso dei like
    public static double wb = 0.7; // peso dei commenti indiretti
    public static int topN = 10;
}
