import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/4/15.
 */
public class RentVsOwned implements JobType {

    private static final int OWNED_START = 1803;
    private static final int OWNED_END = OWNED_START + 9;

    private static final int RENTED_START = 1812;
    private static final int RENTED_END = RENTED_START + 9;

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException, NumberFormatException {
        if(!Util.correctSegment(line, 2)) return null;

        String ownedStr = line.substring(OWNED_START, OWNED_END);
        String rentedStr = line.substring(RENTED_START, RENTED_END);

        return Util.convertStringsToInts(ownedStr, rentedStr);
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setInt(AnalysisType.JOB_TYPE, AnalysisType.OWNED_RENTED);

        Job job = Job.getInstance(conf, "Rent vs Owned");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setReducerClass(FieldsReducer.class);

        return job;
    }

    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        int ownTotal = 0;
        int rentTotal = 0;
        for(IntArrayWritable ownRent : value){
            ownTotal += ownRent.get()[0];
            rentTotal += ownRent.get()[1];
        }

        return ownTotal + " " + rentTotal;
    }
}
