package Reducers;

import JobTypes.JobType;
import Util.Util;
import JobTypes.GenericJob;
import JobTypes.JobTypeFactory;
import Writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class GenericReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

    private GenericJob jobType;

    public void setup(Context context){
        JobTypeFactory jobTypeFactory = JobTypeFactory.getInstance();
        Configuration conf = context.getConfiguration();
        jobType = jobTypeFactory.getJobType(conf.getEnum(Util.JOB_TYPE, JobType.NOTHING));
    }

    public void reduce(Text key, Iterable<IntArrayWritable> value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(jobType.reduce(value)));
    }
}
