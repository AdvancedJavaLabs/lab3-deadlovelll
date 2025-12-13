package sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer
        extends
        Reducer<Text, Text, Text, Text>
{
    private Text result = new Text();

    @Override
    public void reduce(
            Text key,
            Iterable<Text> values,
            Context context
    ) throws
            IOException,
            InterruptedException
    {
        double totalRevenue = 0.0;
        int totalQuantity = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            totalRevenue += Double.parseDouble(parts[0]);
            totalQuantity += Integer.parseInt(parts[1]);
        }

        result.set(totalRevenue + "," + totalQuantity);
        context.write(key, result);
    }
}
