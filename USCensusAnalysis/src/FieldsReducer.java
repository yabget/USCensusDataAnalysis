import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class FieldsReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

    private JobType jobType;

    public void setup(Context context){
        JobTypeFactory jobTypeFactory = JobTypeFactory.getInstance();
        Configuration conf = context.getConfiguration();
        jobType = jobTypeFactory.getJobType(conf.getInt("JobType", 0));
    }

    public void reduce(Text key, Iterable<IntArrayWritable> value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(jobType.reduce(value)));
    }
}
