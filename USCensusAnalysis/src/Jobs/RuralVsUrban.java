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
 * Created by ydubale on 4/7/15.
 */
public class RuralVsUrban implements GenericJob {

    private static final int NUM_FIELDS = 3;
    private static final int FIELD_SIZE = 9;

    private static final int URBAN_IN_URBANIZED_START = 1857;
    private static final int URBAN_IN_URBANIZED_END = URBAN_IN_URBANIZED_START + FIELD_SIZE;

    private static final int URBAN_OUT_URBANIZED_START = 1866;
    private static final int URBAN_OUT_URBANIZED_END = URBAN_OUT_URBANIZED_START + FIELD_SIZE;

    private static final int RURAL_START = 1875;
    private static final int RURAL_END = RURAL_START + FIELD_SIZE;

    @Override
    public Job getJob() throws IOException {
        return null;
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {

        int urban = 0;
        int rural = 0;
        for(IntArrayWritable val : value){
            int[] vals = val.get();

            urban += Util.sumFields(vals, fieldOffset, fieldOffset+1); //Urban_in + Urban_out
            rural += Util.sumFields(vals, fieldOffset+2, fieldOffset+2); //Rural
        }

        context.write(key, new Text("\tRural vs Urban: " + rural + " " + urban + " " ));
    }

    @Override
    public List<IntWritable> getWritable(String line) {
        if(!Util.correctSegment(line, 2)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        String urban_in = line.substring(URBAN_IN_URBANIZED_START, URBAN_IN_URBANIZED_END);
        String urban_out = line.substring(URBAN_OUT_URBANIZED_START, URBAN_OUT_URBANIZED_END);
        String rural = line.substring(RURAL_START, RURAL_END);

        return Util.convertStringsToIntWritables(urban_in, urban_out, rural);
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }
}
