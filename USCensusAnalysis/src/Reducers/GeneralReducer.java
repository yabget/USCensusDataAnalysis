package Reducers;

import JobTypes.GenericJob;
import JobTypes.JobType;
import JobTypes.JobTypeFactory;
import Writables.IntArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ydubale on 4/23/15.
 */
public class GeneralReducer extends Reducer<Text, IntArrayWritable, WritableComparable, Writable> {



    public void reduce(Text key, Iterable<IntArrayWritable> value, Context context) throws IOException, InterruptedException {


        List<IntArrayWritable> fields = new LinkedList<>();

        // Create a copy of the values, in order to iterate through them more than once
        for(IntArrayWritable v : value){
            fields.add(new IntArrayWritable(v.get()));
        }

        int fieldOffset = 0;
        for(int i=0; i < JobType.NUM_JOBS ; i++){
            GenericJob genericJob = JobTypeFactory.getInstance().getJobType(i);
            genericJob.reduce(key, fields, context, fieldOffset);
            fieldOffset += genericJob.getNumFields(); //Offest for the IntArrayWritable
        }

    }
}
