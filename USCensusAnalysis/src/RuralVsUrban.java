import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/7/15.
 */
public class RuralVsUrban implements JobType {

    private static final int URBAN_IN_URBANIZED_START = 1857;
    private static final int URBAN_IN_URBANIZED_END = URBAN_IN_URBANIZED_START + 9;

    private static final int URBAN_OUT_URBANIZED_START = 1866;
    private static final int URBAN_OUT_URBANIZED_END = URBAN_OUT_URBANIZED_START + 9;

    private static final int RURAL_START = 1875;
    private static final int RURAL_END = RURAL_START + 9;

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException {
        if(!Util.correctSegment(line, 2)) return null;

        String urban_in = line.substring(URBAN_IN_URBANIZED_START, URBAN_IN_URBANIZED_END);
        String urban_out = line.substring(URBAN_OUT_URBANIZED_START, URBAN_OUT_URBANIZED_END);
        String rural = line.substring(RURAL_START, RURAL_END);

        return Util.convertStringsToInts(urban_in, urban_out, rural);
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setEnum(Util.JOB_TYPE, AnalysisType.RURAL_URBAN);

        Job job = Job.getInstance(conf, "Urban vs Rural");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setReducerClass(FieldsReducer.class);

        return job;
    }

    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        int urban = 0;
        int rural = 0;

        for(IntArrayWritable val : value){
            urban += (val.get()[0] + val.get()[1]);
            rural += val.get()[2];
        }

        return rural + " " + urban;
    }
}
