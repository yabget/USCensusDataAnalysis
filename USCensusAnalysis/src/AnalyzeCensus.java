import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class AnalyzeCensus {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = null;

        switch (args[0]){
            case "RvO":
                job = (new RentVsOwned()).getJob();
                break;
            case "NM":
                job = (new NeverMarried()).getJob();
                break;
            case "GAD":
                job = (new GenderAgeDistribution()).getJob();
                break;
            default:
                Util.printValidCommands();
                System.exit(1);
        }

        job.setJarByClass(AnalyzeCensus.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
