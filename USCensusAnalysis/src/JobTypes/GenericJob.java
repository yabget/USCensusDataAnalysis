package JobTypes;

import Writables.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by ydubale on 4/6/15.
 */
public interface GenericJob {
    public Job getJob() throws IOException;
    public void reduce(Text key, List<IntArrayWritable> value, Reducer.Context context, int fieldOffset) throws IOException, InterruptedException;
    public List<IntWritable> getWritable(String line);
    public int getNumFields();
}
