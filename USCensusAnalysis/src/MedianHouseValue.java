import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ydubale on 4/8/15.
 */
public class MedianHouseValue implements JobType {

    private static final int HOUSE_VALUE_START = 2928;
    private static final int HOUSE_VALUE_END = HOUSE_VALUE_START + (20 * 9);

    private static final String[] houseCosts = {
            "Less than $15,000",
            "$15,000 - $19,999",
            "$20,000 - $24,999",
            "$25,000 - $29,999",
            "$30,000 - $34,999",
            "$35,000 - $39,999",
            "$40,000 - $44,999",
            "$45,000 - $49,999",
            "$50,000 - $59,999",
            "$60,000 - $74,999",
            "$75,000 - $99,999",
            "$100,000 - $124,999",
            "$125,000 - $149,999",
            "$150,000 - $174,999",
            "$175,000 - $199,999",
            "$200,000 - $249,999",
            "$250,000 - $299,999",
            "$300,000 - $399,999",
            "$400,000 - $499,999",
            "$500,000 or more"
    };

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException {
        if(Integer.parseInt(line.substring(24, 28)) == 1){
            return null;
        }

        int[] fields = new int[20];
        int index = 0;
        for(int i=HOUSE_VALUE_START; i < HOUSE_VALUE_END; i+=9){
            fields[index] = Integer.parseInt(line.substring(i, i+9));
            index++;
        }
        return fields;
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setInt(AnalysisType.JOB_TYPE, AnalysisType.MEDIAN_HOUSE_VALUE);

        Job job = Job.getInstance(conf, "Median House Value");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setPartitionerClass(MHVPartioner.class);

        job.setNumReduceTasks(50);

        job.setReducerClass(MHVReducer.class);

        return job;
    }

    public static class MHVPartioner extends Partitioner<Text, IntArrayWritable>{

        @Override
        public int getPartition(Text text, IntArrayWritable intArrayWritable, int i) {
            return text.toString().hashCode() % i;
        }
    }

    public static class MHVReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

        private Map<Text, int[]> stateToVals = new HashMap<>();

        public void reduce(Text key, Iterable<IntArrayWritable> value, Context context) throws IOException, InterruptedException {

            int[] houseVals = stateToVals.get(key);
            if(houseVals == null){
                houseVals = new int[20];
            }

            for(IntArrayWritable field : value){
                int[] nums = field.get();
                for(int i=0; i< nums.length; i++){
                    houseVals[i]+= nums[i];
                }
            }

            stateToVals.put(key, houseVals);
        }


        public void cleanup(Context context) throws IOException, InterruptedException {

            for(Text state : stateToVals.keySet()){
                //context.write(state, new IntArrayWritable(stateToVals.get(state)));
                long sum = 0;
                int[] houseValues = stateToVals.get(state);

                for(int hVal : houseValues){
                    sum += hVal;
                }

                long midVal = sum/2;

                long newSum = 0;
                for(int i=0; i < houseValues.length; i++){
                    if(newSum > midVal){
                        context.write(state, new Text(houseCosts[i]));
                        break;
                    }
                    newSum += houseValues[i];
                }
            }
        }

    }

    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        return null;
    }
}
