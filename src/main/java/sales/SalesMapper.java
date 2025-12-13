package sales;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper
        extends
        Mapper<LongWritable, Text, Text, Text>
{

    private Text category = new Text();
    private Text revenueAndQty = new Text();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws
            IOException,
            InterruptedException
    {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("transaction_id")) return;

        String[] fields = line.split(",");
        if (fields.length < 5) return;

        String cat = fields[2];
        double price;
        int quantity;
        try {
            price = Double.parseDouble(fields[3]);
            quantity = Integer.parseInt(fields[4]);
        } catch (NumberFormatException e) {
            return;
        }

        if (quantity == 0) return;

        category.set(cat);
        revenueAndQty.set(price * quantity + "," + quantity);
        context.write(category, revenueAndQty);
    }
}
