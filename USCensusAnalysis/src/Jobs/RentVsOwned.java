package Jobs;

import JobTypes.GenericJob;
import Util.Util;
import Writables.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by ydubale on 4/4/15.
 */
public class RentVsOwned implements GenericJob {

    private static final int NUM_FIELDS = 2;
    private static final int FIELD_SIZE = 9;

    private static final int OWNED_START = 1803;
    private static final int OWNED_END = OWNED_START + FIELD_SIZE;

    private static final int RENTED_START = 1812;
    private static final int RENTED_END = RENTED_START + FIELD_SIZE;

    @Override
    public Job getJob() throws IOException {
        return null;
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {
        int ownT = 0;
        int rentT = 0;

        for(IntArrayWritable vals : value){
            int[] fields = vals.get();

            ownT += Util.sumFields(fields, fieldOffset, fieldOffset);
            rentT += Util.sumFields(fields, fieldOffset+1, fieldOffset+1);
        }

        context.write(key, new Text("\tRent vs Owned: " + ownT + " " + rentT));
    }

    @Override
    public List<IntWritable> getWritable(String line) {
        if(!Util.correctSegment(line, 2)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        String ownedStr = line.substring(OWNED_START, OWNED_END);
        String rentedStr = line.substring(RENTED_START, RENTED_END);

        return Util.convertStringsToIntWritables(ownedStr, rentedStr);
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }
}
