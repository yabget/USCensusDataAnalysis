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
public class MedianHouseValue implements GenericJob, MedianJob {

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
    public Job getJob() throws IOException {
        return null;
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {
        int index = Util.generalMedianReducer(value, fieldOffset, NUM_FIELDS);
        if(index != -1){
            context.write(key, new Text("\tMedian house value: " + houseCosts[index]));
        }
    }

    @Override
    public List<IntWritable> getWritable(String line) {
        if(!Util.correctSegment(line, SEGMENT)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        List<IntWritable> list = new LinkedList<>();

        for(int i=HOUSE_VALUE_START; i < HOUSE_VALUE_END; i+=FIELD_SIZE){
            list.add(new IntWritable(Integer.parseInt(line.substring(i, i+FIELD_SIZE))));
        }

        return list;
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }

    @Override
    public String[] getRangeDescriptions() {
        return houseCosts;
    }
}
