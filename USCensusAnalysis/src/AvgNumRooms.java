import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by ydubale on 4/9/15.
 */
public class AvgNumRooms implements JobType {

    private static final int SEGMENT = 2;

    private static final int NUM_FIELDS = 9;
    private static final int FIELD_SIZE = 9;

    private static final int NUM_ROOMS_START = 2388;
    private static final int NUM_ROOMS_END = NUM_ROOMS_START + (NUM_FIELDS * FIELD_SIZE);

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException {
        if(!Util.correctSegment(line, SEGMENT)) return null;

        //Last index holds the sum
        int sum = 0;
        int index = 1;
        int numOfThings = 0;
        for(int i = NUM_ROOMS_START; i < NUM_ROOMS_END; i+=9){
            int val = Integer.parseInt(line.substring(i, i+FIELD_SIZE));
            numOfThings += val;
            sum += (val * index); // Val * num_room
            index++;
        }

        int[] avg = {sum, numOfThings};

        return avg;
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setEnum(Util.JOB_TYPE, AnalysisType.AVG_NUM_ROOMS);

        Job job = Job.getInstance(conf, "Avg Num Rooms");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setNumReduceTasks(1);

        job.setReducerClass(AvgNumRoomsReducer.class);

        return job;
    }

    public static class AvgNumRoomsReducer extends Reducer<Text, IntArrayWritable, DoubleWritable, NullWritable>{

        private ArrayList<Double> averages = new ArrayList<>();

        public void reduce(Text key, Iterable<IntArrayWritable> value, Context context) throws IOException, InterruptedException {
            double avg = 0;
            for(IntArrayWritable val : value){
                avg = (double)val.get()[0]/val.get()[1];
            }
            averages.add(avg);
            context.write(new DoubleWritable(avg), NullWritable.get());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(averages);

            double percentile95th = averages.get((int)(averages.size() * .95));

            context.write(new DoubleWritable(percentile95th), NullWritable.get());
        }
    }

    //Gets the average per state
    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        double avg = 0;
        for(IntArrayWritable val : value){
            avg = (double)val.get()[0]/val.get()[1];
        }
        return avg +"";
    }
}
