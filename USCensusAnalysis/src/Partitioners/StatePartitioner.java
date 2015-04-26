package Partitioners;

import Writables.IntArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ydubale on 4/8/15.
 */
public class StatePartitioner extends Partitioner<Text, IntArrayWritable> {

    /**
     * Partitions the keys into n Number of reducers,
     * With states, this would be partitioned into 50 states
     * @param text
     * @param intArrayWritable
     * @param numReducers
     * @return
     */
    @Override
    public int getPartition(Text text, IntArrayWritable intArrayWritable, int numReducers) {
        return text.toString().hashCode() % numReducers;
    }
}