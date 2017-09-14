package bolts;

import Utils.CuriosityComScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.communication.RoverStatusOuterClass;

import java.util.Map;

public class InterceptConsole extends BaseRichBolt {
    private Logger logger = LoggerFactory.getLogger(InterceptConsole.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        RoverStatusOuterClass.RoverStatus roverStatus = (RoverStatusOuterClass.RoverStatus) tuple.getValueByField
                (CuriosityComScheme.CURIOSITY_MESSAGES);
        System.out.println("======================================================================");
        logger.info(roverStatus.toString());
        System.out.println("======================================================================");
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
