package Partitioners;

import Writables.IntArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ydubale on 4/8/15.
 */
public class StatePartitioner extends Partitioner<Text, IntArrayWritable> {

    @Override
    public int getPartition(Text text, IntArrayWritable intArrayWritable, int numReducers) {
        return text.toString().hashCode() % numReducers;
    }
}