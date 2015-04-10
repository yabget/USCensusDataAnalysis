package Reducers;

import JobTypes.GenericJob;
import JobTypes.JobType;
import JobTypes.JobTypeFactory;
import JobTypes.MedianJob;
import Util.Util;
import Writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ydubale on 4/8/15.
 */
public class MedianReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

    private GenericJob jobType;
    private Map<Text, int[]> stateToVals = new HashMap<>();

    public void setup(Context context){
        JobTypeFactory jobTypeFactory = JobTypeFactory.getInstance();
        Configuration conf = context.getConfiguration();
        jobType = jobTypeFactory.getJobType(conf.getEnum(Util.JOB_TYPE, JobType.NOTHING));
    }

    public void reduce(Text key, Iterable<IntArrayWritable> value, Context context) throws IOException, InterruptedException {

        int[] values = stateToVals.get(key);
        if(values == null){
            if(jobType instanceof MedianJob){
                //Get the range of values
                values = new int[((MedianJob) jobType).getNumRanges()];
                stateToVals.put(key, values);
            }
            else{
                System.exit(1);
            }
        }

        for(IntArrayWritable field : value){
            int[] nums = field.get();
            for(int i=0; i< nums.length; i++){
                values[i]+= nums[i];
            }
        }
    }


    public void cleanup(Context context) throws IOException, InterruptedException {

        for(Text state : stateToVals.keySet()){
            //context.write(state, new IntArrayWritable(stateToVals.get(state)));
            long sum = 0;
            int[] values = stateToVals.get(state);

            for(int val : values){
                sum += val;
            }

            long midVal = sum/2;

            long newSum = 0;

            for(int i=0; i < values.length; i++){
                if(newSum > midVal){
                    if(jobType instanceof MedianJob){
                        String[] rangeDesc = ((MedianJob) jobType).getRangeDescriptions();
                        context.write(state, new Text(rangeDesc[i]));break;
                    }
                    else{
                        System.exit(2);
                    }
                }
                newSum += values[i];
            }
        }
    }
}
