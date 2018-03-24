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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashTagMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable value = new IntWritable(1);
    private Text hashtag = new Text();

    private final static int FILTER = 140;
    private static final int POPULAR_HOUR = 1;

    public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

        String[] arrList = input.toString().split(";");

        if (arrList.length == 4 && arrList[2].length() <= FILTER) {
            try {
                LocalDateTime date = LocalDateTime.ofEpochSecond(Long.parseLong(arrList[0]) / 1000, 0, ZoneOffset.UTC);
                Pattern pattern = Pattern.compile("(#\\w+)");
                Matcher matcher = pattern.matcher(arrList[2]);

                if (date.getHour() == POPULAR_HOUR) {
                    while (matcher.find()) {
                        for (int i = 1; i <= matcher.groupCount(); i++) {
                            hashtag.set(matcher.group(i).toLowerCase());
                            context.write(hashtag, value);
                        }
                    }
                }
            } catch (Exception e) {
            }
        }
    }
}
