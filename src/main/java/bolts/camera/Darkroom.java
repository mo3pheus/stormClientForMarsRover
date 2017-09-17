package bolts.camera;

import com.google.protobuf.InvalidProtocolBufferException;
import naasa.gov.mission.control.naasa.gov.mission.control.util.ImageUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.spectrometer.SpectrometerScanOuterClass;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class Darkroom extends BaseRichBolt {
    public static final String          CAMERA_SHOT = "cameraShot";
    private             Logger          logger      = LoggerFactory.getLogger(Darkroom.class);
    private             OutputCollector collector   = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("Preparing darkRoom");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            byte[] imageBytes = tuple.getBinaryByField(CAMERA_SHOT);
            if (imageBytes != null) {
                try {
                    BufferedImage imag  = ImageIO.read(new ByteArrayInputStream(imageBytes));
                    JFrame        frame = new JFrame();
                    frame.setBounds(0, 0, 1000, 1000);
                    frame.getContentPane().add(new ImageUtil(imag));
                    frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
                    frame.setVisible(true);
                    Thread.sleep(3000);
                    frame.dispose();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
