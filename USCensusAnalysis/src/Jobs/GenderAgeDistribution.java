package Jobs;

import JobTypes.GenericJob;
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
 * Created by ydubale on 4/6/15.
 */
public class GenderAgeDistribution implements GenericJob {

    // All age boundaries are inclusive

    private static final int NUM_FIELDS = 6;

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

    @Override
    public Job getJob() throws IOException {
        return null;
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {

        int maleLess18 =0, male20to29 =0, male30to39 = 0;
        int femLess18 = 0, fem20to29= 0, fem30to39 = 0;

        for(IntArrayWritable val : value){
            int[] fields = val.get();

            maleLess18 += Util.sumFields(fields, fieldOffset, fieldOffset);
            male20to29 += Util.sumFields(fields, fieldOffset +1, fieldOffset +1);
            male30to39 += Util.sumFields(fields, fieldOffset +2, fieldOffset +2);
            femLess18 += Util.sumFields(fields, fieldOffset +3, fieldOffset +3);
            fem20to29 += Util.sumFields(fields, fieldOffset +4, fieldOffset +4);
            fem30to39 += Util.sumFields(fields, fieldOffset +5, fieldOffset +5);
        }

        context.write(key,
                new Text("\tGender Age Dist: " + maleLess18 + " " + male20to29 + " " + male30to39  + " "
                        + femLess18 + " " + fem20to29  + " " + fem30to39));

    }

    @Override
    public List<IntWritable> getWritable(String line) {
        if(!Util.correctSegment(line, 1)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        int[] ranges = new int[NUM_FIELDS];
        ranges[0] = Util.getSumRange(line, MALE_UNDER_18_START, MALE_UNDER_18_END);
        ranges[1] = Util.getSumRange(line, MALE_19_TO_29_START, MALE_19_TO_29_END);
        ranges[2] = Util.getSumRange(line, MALE_30_TO_39_START, MALE_30_TO_39_END);

        ranges[3] = Util.getSumRange(line, FEMALE_UNDER_18_START, FEMALE_UNDER_18_END);
        ranges[4] = Util.getSumRange(line, FEMALE_19_TO_29_START, FEMALE_19_TO_29_END);
        ranges[5] = Util.getSumRange(line, FEMALE_30_TO_39_START, FEMALE_30_TO_39_END);


        List<IntWritable> list = new LinkedList<>();
        for(int i=0; i < ranges.length; i++){
            list.add(new IntWritable(ranges[i]));
        }

        return list;
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }
}
