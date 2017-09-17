package bootstrap;

import bolts.camera.Darkroom;
import bolts.diagnostics.HeartbeatMonitor;
import bolts.lidar.ObstacleDetector;
import bolts.radar.RadarChart;
import bolts.spectrometer.SpectrometerConsole;
import utils.CuriosityComScheme;
import bolts.InterceptConsole;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

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
                ("kafka_spout").setDebug(false);
        topologyBuilder.setBolt("spectrometerBolt", new SpectrometerConsole()).globalGrouping
                ("preliminaryFilterBolt", "SpectrometerConsole").setDebug(false);
        topologyBuilder.setBolt("radarBolt", new RadarChart()).globalGrouping
                ("preliminaryFilterBolt", "RadarChart").setDebug(false);
        topologyBuilder.setBolt("lidarBolt", new ObstacleDetector()).globalGrouping
                ("preliminaryFilterBolt", "ObstacleDetector").setDebug(false);
        topologyBuilder.setBolt("diagnosticsBolt", new HeartbeatMonitor()).globalGrouping
                ("preliminaryFilterBolt", "HeartbeatMonitor").setDebug(false);
        topologyBuilder.setBolt("cameraBolt", new Darkroom()).globalGrouping
                ("preliminaryFilterBolt", "Darkroom").setDebug(false);

        config.setNumWorkers(6);
        config.setMaxSpoutPending(5000);
        StormSubmitter.submitTopology("myTopology", config, topologyBuilder.createTopology());
    }


    public static Properties getKafkaConfig() throws IOException {
        FileInputStream propFile = new FileInputStream
                ("/home/sanket/Documents/workspace/commandcenter/src/main/resources/kafkaProperties.properties");
        Properties kafkaProperties = new Properties();
        kafkaProperties.load(propFile);
        return kafkaProperties;
    }
}
