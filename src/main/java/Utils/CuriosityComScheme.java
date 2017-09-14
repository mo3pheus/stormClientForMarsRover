package Utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.exploration.mars.rover.communication.RoverStatusOuterClass;

import java.nio.ByteBuffer;
import java.util.List;

public class CuriosityComScheme implements Scheme {
    public static final String CURIOSITY_MESSAGES = "Curiosity_Messages1";
    //private             Logger logger             = LoggerFactory.getLogger(CuriosityComScheme.class);

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            System.out.println("======================================================================");
            System.out.println("In the spout!");
            System.out.println(RoverStatusOuterClass.RoverStatus.parseFrom(ByteString.copyFrom(byteBuffer)
                                                                                   .toByteArray()));
            System.out.println("======================================================================");
            return new Values(RoverStatusOuterClass.RoverStatus.parseFrom(ByteString.copyFrom(byteBuffer)
                                                                                  .toByteArray()));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("This is your code puking!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(CURIOSITY_MESSAGES);
    }
}

