package JobTypes;

import Writables.IntArrayWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public interface GenericJob {
    public int[] map(String line) throws StringIndexOutOfBoundsException;
    public Job getJob() throws IOException;
    public String reduce(Iterable<IntArrayWritable> value);
}