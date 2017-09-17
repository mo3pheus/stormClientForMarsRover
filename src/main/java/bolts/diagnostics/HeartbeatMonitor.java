package bolts.diagnostics;

import bolts.spectrometer.SpectrometerConsole;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.diagnostics.HeartBeatOuterClass;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HeartbeatMonitor extends BaseRichBolt {
    public static final String
                             HEARTBEAT         = "curiosityHeartbeat";
    private             Logger
                             logger            = LoggerFactory.getLogger(HeartbeatMonitor.class);
    private             OutputCollector
                             collector         = null;
    private             long timeSinceLastBeat = 0l;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("Prepared diagnostics bolt.");
        this.collector = outputCollector;
        this.timeSinceLastBeat = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            HeartBeatOuterClass.HeartBeat heartBeat = HeartBeatOuterClass.HeartBeat.parseFrom(tuple.getBinaryByField
                    (HEARTBEAT));
            logger.info(heartBeat.toString());
            logger.info("Time since last = " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() -
                                                                                       timeSinceLastBeat) + " minutes");
            timeSinceLastBeat = System.currentTimeMillis();
        } catch (InvalidProtocolBufferException ipe) {
            logger.error("Corrupted message, Expecting message of the type Heartbeat", ipe);
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
