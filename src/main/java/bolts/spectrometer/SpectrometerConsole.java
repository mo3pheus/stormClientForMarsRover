package bolts.spectrometer;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass.SpectrometerScan;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass.SpectrometerScan.Location;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass.SpectrometerScan.Composition;

import java.util.HashMap;
import java.util.Map;

public class SpectrometerConsole extends BaseRichBolt {
    public static final int                        MAX_CELLS         = 784;
    public static final String                     SPECTROMETER_DATA = "SpectrometerScanData";
    private             Logger                     logger            = LoggerFactory.getLogger(SpectrometerConsole
                                                                                                       .class);
    private             Map<Location, Composition> marsSurface       = new HashMap<>();
    private             OutputCollector            collector         = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("Prepared spectrometer bolt.");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            SpectrometerScan spectrometerScan = SpectrometerScan.parseFrom(tuple.getBinaryByField(SPECTROMETER_DATA));
            logger.info(spectrometerScan.toString());

            for (SpectrometerScan.PointComp pointComp : spectrometerScan.getScanAreaCompList()) {
                marsSurface.put(pointComp.getPoint(), pointComp.getSoilComp());
            }

            double percentCoverage = ((double) marsSurface.keySet().size() / MAX_CELLS) * 100.0d;
            logger.info("Mars Surface data " + percentCoverage + "% complete");
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
