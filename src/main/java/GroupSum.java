import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GroupSum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<String, String, Integer>> inputDataStream = environment.fromElements(
                new Tuple3<>("Yang", "English", 95),
                new Tuple3<>("Li", "Math", 87),
                new Tuple3<>("Zhang", "Math", 62),
                new Tuple3<>("Zhang", "English", 92),
                new Tuple3<>("Yang", "Math", 90),
                new Tuple3<>("Li", "English", 73)
        );

        DataStream output = inputDataStream;

        output.print();

        environment.execute();
    }
}
