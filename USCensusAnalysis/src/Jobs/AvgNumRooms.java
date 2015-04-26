package Jobs;

import JobTypes.GenericJob;
import JobTypes.JobType;
import Util.Util;
import Writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ydubale on 4/9/15.
 */
public class AvgNumRooms implements GenericJob {

    private static final int SEGMENT = 2;

    private static final int NUM_FIELDS = 9;
    private static final int FIELD_SIZE = 9;

    private static final int NUM_ROOMS_START = 2388;
    private static final int NUM_ROOMS_END = NUM_ROOMS_START + (NUM_FIELDS * FIELD_SIZE);

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setInt(Util.JOB_TYPE, JobType.AVG_NUM_ROOMS);

        Job job = Job.getInstance(conf, "Avg Num Rooms");

        job.setMapperClass(AvgNumRoomsMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setNumReduceTasks(1);

        job.setReducerClass(AvgNumRoomsReducer.class);

        return job;
    }

    public static class AvgNumRoomsMapper extends Mapper<LongWritable, Text, NullWritable, IntArrayWritable> {

        private static final AvgNumRooms avgNumRooms = new AvgNumRooms();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            int sumLevel = Integer.parseInt(line.substring(10, 13));

            if(sumLevel != 100){ //Make sure summary level is 100
                return;
            }

            context.write(NullWritable.get(), new IntArrayWritable(avgNumRooms.getWritable(line)));
        }
    }

    public static class AvgNumRoomsReducer extends Reducer<NullWritable, IntArrayWritable, Text, DoubleWritable> {

        private List<Double> averages = new LinkedList<>();

        public void reduce(NullWritable key, Iterable<IntArrayWritable> value, Context context){
            int[] values = new int[NUM_FIELDS];

            for(IntArrayWritable field : value){
                int[] nums = field.get();

                for (int i=0; i < NUM_FIELDS; i++){
                    values[i] += nums[i];
                }
            }

            long sum =0;
            long totalCount = 0;

            int numRoom = 1;
            for(int val : values){
                //context.write(new Text(numRoom + " room:"), new LongWritable(val));
                totalCount += val;
                sum += val * numRoom;
                numRoom++;
            }

            double avg = (double) sum / totalCount;
            averages.add(avg);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(averages);

            int percentile95th = (int)(averages.size() * 0.95);

            context.write(new Text("95th percentile of the average number of rooms per house across all states"),
                    new DoubleWritable(averages.get(percentile95th)));

        }
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {
    }

    @Override
    public List<IntWritable> getWritable(String line) {

        if(!Util.correctSegment(line, SEGMENT)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        List<IntWritable> list = new LinkedList<>();
        for(int i=NUM_ROOMS_START; i < NUM_ROOMS_END; i+= FIELD_SIZE){
            list.add(new IntWritable(Integer.parseInt(line.substring(i, i+FIELD_SIZE))));
        }

        return list;
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }
}
