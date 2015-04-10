import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ydubale on 4/9/15.
 */
public class ElderlyPeople implements JobType {

    private static final int SEGMENT = 1;

    private static final int FIELD_SIZE = 9;

    private static final int OVER_85_START = 1065;
    private static final int OVER_85_END = OVER_85_START + FIELD_SIZE;

    private static final int TOTAL_PERSONS_START = 300;
    private static final int TOTAL_PERSONS_END = TOTAL_PERSONS_START + FIELD_SIZE;

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException {
        if(!Util.correctSegment(line, SEGMENT)) return null;

        String over_85 = line.substring(OVER_85_START, OVER_85_END);
        String total = line.substring(TOTAL_PERSONS_START, TOTAL_PERSONS_END);

        if(Integer.parseInt(total) == 0) return null;

        return Util.convertStringsToInts(over_85, total);
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setInt(AnalysisType.JOB_TYPE, AnalysisType.ELDERLY_PEOPLE);

        Job job = Job.getInstance(conf, "Elderly People");

        job.setMapperClass(ElderlyPeopleMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);

        job.setMapOutputValueClass(MapWritable.class);

        job.setNumReduceTasks(1);

        job.setReducerClass(ElderlyPeopleReducer.class);

        return job;
    }

    public static class ElderlyPeopleMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {

        private Map<Text, int[]> stateToFields = new HashMap<>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String sumLevelStr = line.substring(10, 13);

            int sumLevel = Integer.parseInt(sumLevelStr);

            if(sumLevel != 100){
                return;
            }

            Text state = new Text(line.substring(8, 10));

            int[] outVals;
            try {
                if(!Util.correctSegment(line, SEGMENT)) return;

                String over_85 = line.substring(OVER_85_START, OVER_85_END);
                String total = line.substring(TOTAL_PERSONS_START, TOTAL_PERSONS_END);

                outVals = Util.convertStringsToInts(over_85, total);

                if(outVals == null || outVals[1] == 0){
                    return;
                }

                int[] over85_total = stateToFields.get(state);
                if(over85_total == null){
                    over85_total = new int[2];
                    stateToFields.put(state, over85_total);
                }

                over85_total[0] += outVals[0];
                over85_total[1] += outVals[1];
            }
            catch (NumberFormatException | StringIndexOutOfBoundsException ignored){
            }
        }

        private double getPercentage(int... values){
            return (double) values[0]/values[1];
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Text maxState = new Text();
            double max = 0;
            for(Text state : stateToFields.keySet()){
                double tempPerc = getPercentage(stateToFields.get(state));
                if(tempPerc > max){
                    max = tempPerc;
                    maxState = state;
                }
                //context.write(state, new DoubleWritable(tempPerc));
            }
            MapWritable myMap = new MapWritable();
            for(Text state : stateToFields.keySet()){
                double tempPerc = getPercentage(stateToFields.get(state));
                myMap.put(state, new DoubleWritable(tempPerc));
            }
            //context.write(maxState, new MapWritable(stateToFields));
            context.write(NullWritable.get(), myMap);
        }
    }


    public static class ElderlyPeopleReducer extends Reducer<NullWritable,MapWritable, Text, DoubleWritable> {

        private Text maxState = new Text();
        private double max = 0;

        public void reduce(NullWritable key, Iterable<MapWritable> value, Context context) throws IOException, InterruptedException {

            for(MapWritable val : value){
                Set<Writable> states = val.keySet();
                for(Writable state : states){
                    Writable tempPercentage = val.get(state);
                    double tempMax = ((DoubleWritable) tempPercentage).get();
                    if(tempMax > max){
                        max = tempMax;
                        maxState = (Text)state;
                    }
                    //context.write((Text)state, new DoubleWritable(tempMax));
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maxState, new DoubleWritable(max));
        }
    }


    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        return null;
    }
}
