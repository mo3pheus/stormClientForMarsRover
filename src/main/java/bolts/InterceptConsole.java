package bolts;

import bolts.camera.Darkroom;
import bolts.diagnostics.HeartbeatMonitor;
import bolts.lidar.ObstacleDetector;
import bolts.radar.RadarChart;
import bolts.spectrometer.SpectrometerConsole;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import space.exploration.mars.rover.diagnostics.HeartBeatOuterClass;
import space.exploration.mars.rover.kernel.ModuleDirectory;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass;
import utils.CuriosityComScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.communication.RoverStatusOuterClass;

import java.util.HashMap;
import java.util.Map;

public class InterceptConsole extends BaseRichBolt {
    private Logger logger = LoggerFactory.getLogger(InterceptConsole.class);
    private OutputCollector collector;
    private Map<Integer, String> streamFilterMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        streamFilterMap.put(ModuleDirectory.Module.CAMERA_SENSOR.getValue(), "Darkroom");
        streamFilterMap.put(ModuleDirectory.Module.SENSOR_LIDAR.getValue(), "ObstacleDetector");
        streamFilterMap.put(ModuleDirectory.Module.RADAR.getValue(), "RadarChart");
        streamFilterMap.put(ModuleDirectory.Module.SCIENCE.getValue(), "SpectrometerConsole");
        streamFilterMap.put(ModuleDirectory.Module.DIAGNOSTICS.getValue(), "HeartbeatMonitor");
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple == null) {
            logger.error("Tuple was null.");
        }

        RoverStatusOuterClass.RoverStatus roverStatus = null;
        try {
            roverStatus = RoverStatusOuterClass.RoverStatus.parseFrom(tuple.getBinaryByField(CuriosityComScheme
                                                                                                     .CURIOSITY_MESSAGES));
            logger.info(roverStatus.toString());

            if (streamFilterMap.containsKey(roverStatus.getModuleReporting())) {
                collector.emit(streamFilterMap.get(roverStatus.getModuleReporting()),
                               new Values(roverStatus.getModuleMessage().toByteArray()));
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Invalid protocol buffer", e);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("SpectrometerConsole", new Fields(SpectrometerConsole.SPECTROMETER_DATA));
        outputFieldsDeclarer.declareStream("Darkroom", new Fields(Darkroom.CAMERA_SHOT));
        outputFieldsDeclarer.declareStream("HeartbeatMonitor", new Fields(HeartbeatMonitor.HEARTBEAT));
        outputFieldsDeclarer.declareStream("ObstacleDetector", new Fields(ObstacleDetector.LIDAR_DATA));
        outputFieldsDeclarer.declareStream("RadarChart", new Fields(RadarChart.RADAR_DATA));
    }
}
