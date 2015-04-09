import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class GenderAgeDistribution implements JobType {

    // All age boundaries are inclusive

    private static final int MALE_UNDER_18_START = 3864;
    private static final int MALE_UNDER_18_END = MALE_UNDER_18_START + (13 * 9);

    private static final int MALE_19_TO_29_START = MALE_UNDER_18_END;
    private static final int MALE_19_TO_29_END = MALE_19_TO_29_START + (5 * 9);

    private static final int MALE_30_TO_39_START = MALE_19_TO_29_END;
    private static final int MALE_30_TO_39_END = MALE_30_TO_39_START + (2 * 9);

    private static final int FEMALE_UNDER_18_START = 4143;
    private static final int FEMALE_UNDER_18_END = FEMALE_UNDER_18_START + (13 * 9);

    private static final int FEMALE_19_TO_29_START = FEMALE_UNDER_18_END;
    private static final int FEMALE_19_TO_29_END = FEMALE_19_TO_29_START + (5 * 9);

    private static final int FEMALE_30_TO_39_START = FEMALE_19_TO_29_END;
    private static final int FEMALE_30_TO_39_END = FEMALE_30_TO_39_START + (2 * 9);

    private int getSumAgeRange(String line, int start, int end){
        int sum = 0;
        for(int i= start; i <= end; i+=9){
            sum += Integer.parseInt(line.substring(i, i+9));
        }
        return sum;
    }

    @Override
    public int[] getFields(String line) throws StringIndexOutOfBoundsException {
        if(Integer.parseInt(line.substring(24, 28)) == 2){
            return null;
        }

        int[] ranges = new int[6];
        ranges[0] = getSumAgeRange(line, MALE_UNDER_18_START, MALE_UNDER_18_END);
        ranges[1] = getSumAgeRange(line, MALE_19_TO_29_START, MALE_19_TO_29_END);
        ranges[2] = getSumAgeRange(line, MALE_30_TO_39_START, MALE_30_TO_39_END);

        ranges[3] = getSumAgeRange(line, FEMALE_UNDER_18_START, FEMALE_UNDER_18_END);
        ranges[4] = getSumAgeRange(line, FEMALE_19_TO_29_START, FEMALE_19_TO_29_END);
        ranges[5] = getSumAgeRange(line, FEMALE_30_TO_39_START, FEMALE_30_TO_39_END);

        return ranges;
    }

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();

        conf.setInt(AnalysisType.JOB_TYPE, AnalysisType.GENDER_AGE_DIST);

        Job job = Job.getInstance(conf, "Gender Age Dist");

        job.setMapperClass(FieldsMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setReducerClass(FieldsReducer.class);

        return job;
    }

    @Override
    public String reduce(Iterable<IntArrayWritable> value) {
        int maleLess18 =0, male20to29 =0, male30to39 = 0;
        int femLess18 = 0, fem20to29= 0, fem30to39 = 0;

        for(IntArrayWritable field : value){
            maleLess18 += field.get()[0];
            male20to29 += field.get()[1];
            male30to39 += field.get()[2];
            femLess18 += field.get()[3];
            fem20to29 += field.get()[4];
            fem30to39 += field.get()[5];
        }

        return maleLess18 + " " + male20to29 + " " + male30to39  + " " + femLess18 + " " + fem20to29  + " " + fem30to39;
    }
}
