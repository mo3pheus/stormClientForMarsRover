package bolts.lidar;

import bolts.spectrometer.SpectrometerConsole;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.communication.EarthProtocol;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass;

import java.util.HashMap;
import java.util.Map;

public class ObstacleDetector extends BaseRichBolt {
    public static final String          LIDAR_DATA = "lidarData";
    private             Logger          logger     = LoggerFactory.getLogger(ObstacleDetector.class);
    private             OutputCollector collector  = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("Prepared lidar bolt.");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            EarthProtocol.RoverPing roverPing = EarthProtocol.RoverPing.parseFrom(tuple.getBinaryByField(LIDAR_DATA));
            logger.info(roverPing.toString());
        } catch (InvalidProtocolBufferException ipe) {
            logger.error("Corrupted message, Expecting message of the type SpectrometerScan", ipe);
        } catch (Exception e) {
            logger.error("Uncaught Exception", e);
        } finally {
            collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
