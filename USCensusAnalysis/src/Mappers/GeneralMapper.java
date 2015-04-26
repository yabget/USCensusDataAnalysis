package Mappers;

import JobTypes.GenericJob;
import JobTypes.JobType;
import JobTypes.JobTypeFactory;
import Writables.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ydubale on 4/20/15.
 */

/**
 * For a single line, writes out the fields that need to be analyzed in one big array of ints
 */
public class GeneralMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        int sumLevel = Integer.parseInt(line.substring(10, 13));

        if(sumLevel != 100){ //Make sure summary level is 100
            return;
        }

        String state = line.substring(8, 10);

        List<IntWritable> allFields = new LinkedList<>();
        for(int i=0; i < JobType.NUM_JOBS; i++){
            GenericJob genericJob = JobTypeFactory.getInstance().getJobType(i);
            allFields.addAll(genericJob.getWritable(line));
        }

        context.write(new Text(state), new IntArrayWritable(allFields));
    }

}
