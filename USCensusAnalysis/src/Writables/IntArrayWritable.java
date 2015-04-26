package Writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Created by ydubale on 4/4/15.
 */
public class IntArrayWritable implements Writable {

    private int[] values;

    public IntArrayWritable(){
        values = new int[0];
    }

    public IntArrayWritable(int[] values){
        this.values = values;
    }

    public IntArrayWritable(List<IntWritable> intWritables){
        values = new int[intWritables.size()];
        int index = 0;
        for(IntWritable i : intWritables){
            values[index] = i.get();
            index++;
        }
    }

    public int[] get(){
        return values;
    }

    public String toString(){
        String outp = "";
        for(int intWritable : this.values){
            outp += intWritable + "\t";
        }
        return outp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.values.length);

        for(int intWritable : values){
            dataOutput.writeInt(intWritable);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.values = new int[dataInput.readInt()];

        for(int i = 0; i < this.values.length; ++i){
            this.values[i] = dataInput.readInt();
        }

    }
}