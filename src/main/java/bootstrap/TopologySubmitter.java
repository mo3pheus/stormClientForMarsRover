package bootstrap;

import Utils.CuriosityComScheme;
import bolts.InterceptConsole;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TopologySubmitter {

    public static void main(String[] args) throws Exception {
        Properties      kafkaProperties = getKafkaConfig();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        Config config = new Config();


        SpoutConfig spoutConfig = new SpoutConfig(
                new ZkHosts(kafkaProperties.getProperty("zkConnectString")),
                kafkaProperties.getProperty("topic"),
                kafkaProperties.getProperty("kafka.zkRoot"),
                kafkaProperties.getProperty("id")
        );

        spoutConfig.scheme = new SchemeAsMultiScheme(new CuriosityComScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        System.out.println("======================================================================");
        System.out.println("New kafka spout created.");
        System.out.println("======================================================================");

        topologyBuilder.setSpout("kafka_spout", kafkaSpout, 1);
        topologyBuilder.setBolt("preliminaryFilterBolt", new InterceptConsole()).globalGrouping
                ("kafka_spout").setDebug(true);

        config.setNumWorkers(2);
        config.setMaxSpoutPending(5000);
        StormSubmitter.submitTopology("myTopology", config, topologyBuilder.createTopology());

//        LocalCluster localCluster = new LocalCluster();
//        try {
//            localCluster.submitTopology("houston_to_curiosity_comCenter", config, topologyBuilder.createTopology());
//        } catch (Exception e) {
//            System.out.println("This is the application catching the exception");
//            e.printStackTrace();
//        }
//        Thread.sleep(TimeUnit.DAYS.toMillis(1));
//        localCluster.shutdown();
    }


    public static Properties getKafkaConfig() throws IOException {
        FileInputStream propFile = new FileInputStream
                ("/home/sanket/Documents/workspace/commandcenter/src/main/resources/kafkaProperties.properties");
        Properties kafkaProperties = new Properties();
        kafkaProperties.load(propFile);
        return kafkaProperties;
    }
}
