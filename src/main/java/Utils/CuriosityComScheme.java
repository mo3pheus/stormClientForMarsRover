package Utils;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import space.exploration.mars.rover.communication.RoverStatusOuterClass;

import java.nio.ByteBuffer;
import java.util.List;

public class CuriosityComScheme implements Scheme {
    public static final String CURIOSITY_MESSAGES = "Curiosity_Messages1";

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            System.out.println("In the spout!");
            System.out.println(RoverStatusOuterClass.RoverStatus.parseFrom(byteBuffer.array()));
            return new Values(RoverStatusOuterClass.RoverStatus.parseFrom(byteBuffer.array()));
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

