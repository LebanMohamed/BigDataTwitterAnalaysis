import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.LocalDateTime;
import java.text.DateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private IntWritable value = new IntWritable(1);
    private IntWritable hour = new IntWritable();

    private final static int FILTER = 140;

    public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

        String[] arrList = input.toString().split(";");

        if (arrList.length == 4 && arrList[2].length() <= FILTER) {
            try {
                LocalDateTime date = LocalDateTime.ofEpochSecond(Long.parseLong(arrList[0]) / 1000, 0, ZoneOffset.UTC);
                hour.set(date.getHour());
                context.write(hour, value);
            } catch (Exception e) {
            }
        }
    }

}
