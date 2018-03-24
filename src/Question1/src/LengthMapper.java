import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable value = new IntWritable(1);
    private Text range = new Text();

    private final static int FILTER = 140;
    private static int start = 0, end = 0;

    public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

        String[] arrList = input.toString().split(";");

        if (arrList.length == 4 && arrList[2].length() <= FILTER) {

            int tweetLength = arrList[2].length();

            if (tweetLength % 5 == 0) {
                start = tweetLength - 4;
                end = start + 4;
            } else {
                start = tweetLength - (tweetLength % 5) + 1;
                end = start+ 4;
            }

            range.set(start + "-" + end);
            context.write(range, value);
        }
    }

}
