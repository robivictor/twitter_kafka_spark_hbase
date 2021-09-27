import config.HBaseConfig;
import consumer.KafkaConsumer;
import jobs.SentimentAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import sink.HbaseSink;
import java.util.List;

public class SparkRunner {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Tweets Sentiment Analysis");
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        /**
         * This piece of code does consume the stream of data being produced by Kafka
         * */
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(3));
        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        JavaPairDStream<String, List<String>> stream = kafkaConsumer.consumeStream(jssc);

        /**
         * This is where the piece of algorithm to decide what sentiment each tweet represents.
         * I am simply deciding how positively or negatively each tweet sound by looking on the content of the tweet against a list of
         * and comparing it against a set of predefined positive and negative words.
         * */
        SentimentAnalysis sentimentAnalysis = new SentimentAnalysis(jssc);
        JavaDStream<String> resultSA =  sentimentAnalysis.applySentimentAnalysis(stream);

        /**
         * Once the sentiment analysis is done, we persist the value to HBase
         * */
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", HBaseConfig.zookeeperQuorum);
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", HBaseConfig.zookeeperClientPort);
        JavaHBaseContext javaHBaseContext = new JavaHBaseContext(jsc, hbaseConfiguration);
        HbaseSink.saveToHbase(javaHBaseContext, resultSA);

        /**
         * Start then java spark streaming context(jssc) and listen to exceptions
         * */
        jssc.start();
        try {
            jssc.awaitTermination();
        jsc.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

