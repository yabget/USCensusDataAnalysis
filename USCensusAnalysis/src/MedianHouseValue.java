import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/8/15.
 */
public class MedianHouseValue implements JobType, MedianJob {

    private static final int SEGMENT = 2;

    private static final int NUM_FIELDS = 20;
    private static final int FIELD_SIZE = 9;

    private static final int HOUSE_VALUE_START = 2928;
    private static final int HOUSE_VALUE_END = HOUSE_VALUE_START + (NUM_FIELDS * FIELD_SIZE);

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
        if(!Util.correctSegment(line, SEGMENT)) return null;

        int[] fields = new int[NUM_FIELDS];
        int index = 0;
        for(int i=HOUSE_VALUE_START; i < HOUSE_VALUE_END; i+=FIELD_SIZE){
            fields[index] = Integer.parseInt(line.substring(i, i+FIELD_SIZE));
            index++;
        }
        return fields;
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setEnum(Util.JOB_TYPE, AnalysisType.MEDIAN_HOUSE_VALUE);

        Job job = Job.getInstance(conf, "Median House Value");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setNumReduceTasks(50);

        job.setPartitionerClass(StatePartitioner.class);

        job.setReducerClass(MedianReducer.class);

        return job;
    }

    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        return null;
    }

    @Override
    public int getNumRanges() {
        return NUM_FIELDS;
    }

    @Override
    public String[] getRangeDescriptions() {
        return houseCosts;
    }
}
