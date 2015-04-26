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
 * Created by ydubale on 4/4/15.
 */
public class NeverMarried implements GenericJob {

    private static final int NUM_FIELDS = 4;
    private static final int FIELD_SIZE = 9;

    public static final int MALE_NM_START = 4422;
    public static final int MALE_NM_END = MALE_NM_START + 9;

    public static final int FEMALE_NM_START = 4467;
    public static final int FEMALE_NM_END = FEMALE_NM_START + 9;

    @Override
    public Job getJob() throws IOException {
        return null;
    }

    @Override
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException {
        int maleNM = 0;
        int maleT = 0;
        int femaleNM = 0;
        int femaleT = 0;

        for(IntArrayWritable vals : value){
            int[] fields = vals.get();

            maleNM += Util.sumFields(fields, fieldOffset, fieldOffset);
            maleT += Util.sumFields(fields, fieldOffset+1, fieldOffset +1);
            femaleNM += Util.sumFields(fields, fieldOffset+2, fieldOffset +2);
            femaleT += Util.sumFields(fields, fieldOffset +3, fieldOffset +3);
        }

        context.write(key, new Text("\tNever Married: " + maleNM + " " + maleT + " " + femaleNM + " " + femaleT));
    }

    @Override
    public List<IntWritable> getWritable(String line) {
        if(!Util.correctSegment(line, 1)){
            return Util.getPlaceHolder(NUM_FIELDS);
        }

        String maleNM = line.substring(MALE_NM_START, MALE_NM_END);
        int male = Integer.parseInt(maleNM);

        String femaleNM = line.substring(FEMALE_NM_START, FEMALE_NM_END);
        int female = Integer.parseInt(femaleNM);

        int maleTotal = Util.getSumRange(line, MALE_NM_START, MALE_NM_START + (NUM_FIELDS * FIELD_SIZE));
        int femTotal = Util.getSumRange(line, FEMALE_NM_START, FEMALE_NM_START + (NUM_FIELDS * FIELD_SIZE));

        int[] result = {male, maleTotal, female, femTotal};

        List<IntWritable> list = new LinkedList<>();
        for(int i=0; i < result.length; i++){
            list.add(new IntWritable(result[i]));
        }

        return list;
    }

    @Override
    public int getNumFields() {
        return NUM_FIELDS;
    }

}
