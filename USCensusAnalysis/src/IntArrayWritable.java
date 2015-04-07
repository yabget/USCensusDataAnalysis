import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ydubale on 4/4/15.
 */
public class IntArrayWritable implements WritableComparable {

    private int[] values;

    public IntArrayWritable(){
        values = new int[0];
    }

    public IntArrayWritable(int[] values){
        this.values = values;
    }

    public int[] get(){
        return values;
    }

    /**
     * todo: look into how this can be done
     * @param o
     * @return
     */
    @Override
    public int compareTo(Object o) {
        if(!(o instanceof IntArrayWritable)){
            throw new ClassCastException();
        }
        return 0;
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