package Jobs;

import JobTypes.GenericJob;
import JobTypes.MedianJob;
import Util.Util;
import Writables.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
    public Job getJob() throws IOException {
        return null;
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {
        int index = Util.generalMedianReducer(value, fieldOffset, NUM_FIELDS);
        if(index != -1){
            context.write(key, new Text("\tMedian rent: " + rentRanges[index]));
        }
    }

    @Override
    public List<IntWritable> getWritable(String line) {
        if(!Util.correctSegment(line, SEGMENT)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        List<IntWritable> list= new LinkedList<>();

        for(int i=RENT_START; i < RENT_END; i+=FIELD_SIZE){
            list.add(new IntWritable(Integer.parseInt(line.substring(i, i + FIELD_SIZE))));
        }

        return list;
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }

    @Override
    public String[] getRangeDescriptions() {
        return rentRanges;
    }
}
