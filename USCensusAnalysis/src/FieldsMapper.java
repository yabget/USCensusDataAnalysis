import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class FieldsMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

    private JobType jobType;

    public void setup(Context context){
        JobTypeFactory jobTypeFactory = JobTypeFactory.getInstance();
        Configuration conf = context.getConfiguration();
        jobType = jobTypeFactory.getJobType(conf.getEnum(Util.JOB_TYPE, AnalysisType.NOTHING));
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String sumLevelStr = line.substring(10, 13);

        int sumLevel = Integer.parseInt(sumLevelStr);

        if(sumLevel != 100){
            return;
        }

        String state = line.substring(8, 10);

        int[] outVals;
        try {
            outVals = jobType.getFields(line);
            if(outVals == null){
                return;
            }
        }
        catch (NumberFormatException numForm){
            return;
        }
        catch (StringIndexOutOfBoundsException sout){
            return;
        }

        context.write(new Text(state), new IntArrayWritable(outVals));
    }

}
