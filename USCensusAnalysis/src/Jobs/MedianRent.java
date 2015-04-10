package Jobs;

import JobTypes.GenericJob;
import JobTypes.JobType;
import JobTypes.MedianJob;
import Mappers.GenericMapper;
import Partitioners.StatePartitioner;
import Reducers.MedianReducer;
import Util.Util;
import Writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/8/15.
 */
public class MedianRent implements GenericJob, MedianJob {

    private static final int SEGMENT = 2;

    private static final int NUM_FIELDS = 17;
    private static final int FIELD_SIZE = 9;

    private static final int RENT_START = 3451;
    private static final int RENT_END = RENT_START + (NUM_FIELDS * FIELD_SIZE);

    private static final String[] rentRanges = {
            "Less than $100",
            "$100 to $149",
            "$150 to $199",
            "$200 to $249",
            "$250 to $299",
            "$300 to $349",
            "$350 to $399",
            "$400 to $449",
            "$450 to $499",
            "$500 to $549",
            "$550 to $ 599",
            "$600 to $649",
            "$650 to $699",
            "$700 to $749",
            "$750 to $999",
            "$1000 or more",
            "No cash rent"
    };

    @Override
    public int[] map(String line) throws StringIndexOutOfBoundsException {
        if(!Util.correctSegment(line, SEGMENT)) return null;

        int[] fields = new int[NUM_FIELDS];
        int index = 0;
        for(int i=RENT_START; i < RENT_END; i+=FIELD_SIZE){
            fields[index] = Integer.parseInt(line.substring(i, i+FIELD_SIZE));
            index++;
        }
        return fields;
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setEnum(Util.JOB_TYPE, JobType.MEDIAN_RENT);

        Job job = Job.getInstance(conf, "Median Rent");

        job.setMapperClass(GenericMapper.class);

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
        return rentRanges;
    }
}
