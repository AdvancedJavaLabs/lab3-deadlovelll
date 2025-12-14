package sales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer
        extends
        Reducer<Text, Text, Text, Text>
{
    private Text result = new Text();
    private String metric;

    @Override
    protected void setup(Context context)
            throws
            IOException,
            InterruptedException
    {
        metric = context.getConfiguration().get(
                "analysis.metric",
                "totalRevenue"
        );
    }

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
        double totalPriceSum = 0.0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double revenue = Double.parseDouble(parts[0]);
            int quantity = Integer.parseInt(parts[1]);
            double price = Double.parseDouble(parts[2]);

            totalRevenue += revenue;
            totalQuantity += quantity;
            totalPriceSum += price;
        }

        double avgPrice = totalQuantity > 0 ? totalPriceSum / totalQuantity : 0.0;

        switch (metric) {
            case "totalRevenue":
                result.set(String.valueOf(totalRevenue));
                break;
            case "totalQuantity":
                result.set(String.valueOf(totalQuantity));
                break;
            case "avgPrice":
                result.set(String.valueOf(avgPrice));
                break;
            default:
                result.set(totalRevenue + "," + totalQuantity + "," + avgPrice);
        }

        context.write(key, result);
    }
}
