import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/4/15.
 */
public class NeverMarried implements JobType {

    // NM == never married
    public static final int MALE_NM_START = 4422;
    public static final int MALE_NM_END = MALE_NM_START + 9;

    public static final int FEMALE_NM_START = 4467;
    public static final int FEMALE_NM_END = FEMALE_NM_START + 9;

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException {
        if(!Util.correctSegment(line, 1)) return null;

        String male = line.substring(MALE_NM_START, MALE_NM_END);
        String female = line.substring(FEMALE_NM_START, FEMALE_NM_END);

        return Util.convertStringsToInts(male, female);
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setEnum(Util.JOB_TYPE, AnalysisType.NEVER_MARRIED);

        Job job = Job.getInstance(conf, "Never Married");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setReducerClass(FieldsReducer.class);

        return job;
    }

    public String reduce(Iterable<IntArrayWritable> value){
        int maleT = 0;
        int femaleT = 0;

        for(IntArrayWritable maleFem : value){
            maleT += maleFem.get()[0];
            femaleT += maleFem.get()[1];
        }

        return maleT + " " + femaleT;
    }

}
