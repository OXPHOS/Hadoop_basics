// Calculate the top2 lowest price, value sum and average price
// of in put <Compay, Stock price> file

package OXPHOS.Stock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockPrice {
	public static class StockMapper extends
		Mapper<Object, Text, Text, StockInfo> {
		
		private Text company = new Text();
		private int price;
		private StockInfo stockInfo = new StockInfo();
		
		@Override
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String[] valueString = value.toString().split(",");
			company.set(valueString[0]);
			price = Integer.parseInt(valueString[1]);

			stockInfo.setLow1(price);
			stockInfo.setLow2(price);
			stockInfo.setSum(price);
			stockInfo.setCount(1);
			
			context.write(company, stockInfo);	
		}
	}
	
	// Combiner must have the same Key/Value type as Mapper!
	public static class StockCombiner extends
		Reducer<Text, StockInfo, Text, StockInfo> {
		
		private int count;
		private int sum;
		private int[] lowest = new int[2];
		private StockInfo stockInfo = new StockInfo();
		
		@Override
		public void reduce(Text key, Iterable<StockInfo> 
			values, Context context) throws IOException,
			InterruptedException {
			
			count = 0;
			sum = 0;
			lowest[0] = Integer.MAX_VALUE;
			lowest[1] = Integer.MAX_VALUE;
			for (StockInfo val:values) {
				
				// Initialize two lowest values
				if (count == 0) {
					lowest[0] = val.getSum();
					lowest[1] = val.getSum();
					
				// Make sure lowest[0] <= lowest[1]
				} else if (count == 1) {
					if (lowest[0] < val.getSum()) {
						lowest[1] = val.getSum();
					} else {
						lowest[0] = val.getSum();
					}
				// Replace larger value
				} else {
					if (lowest[1] > val.getLow1()) {
						if (lowest[0] > val.getSum()) {
							lowest[1] = lowest[0];
							lowest[0] = val.getSum();
						} else {
							lowest[1] = val.getSum();
						}
					}
				}
				
				sum += val.getSum();
				count++;
			}
			
			stockInfo.setLow1(lowest[0]);
			stockInfo.setLow2(lowest[1]);
			stockInfo.setSum(sum);
			stockInfo.setCount(count);
			
			context.write(key, stockInfo);
		}	
	}
	
	public static class StockReducer extends
		Reducer<Text, StockInfo, Text, StockSummary> {
		
		int sum;
		int count;
		float average;
		private int[] lowest = new int[2];
		private StockSummary stockSummary = new StockSummary();
		
		@Override
		public void reduce(Text key, Iterable<StockInfo> 
			values, Context context) throws IOException,
			InterruptedException {
			
			sum = 0;
			count = 0;
			average = 0;
			lowest[0] = Integer.MAX_VALUE;
			lowest[1] = Integer.MAX_VALUE;
			
			for (StockInfo val:values) {
				count += val.getCount();
				sum += val.getSum();
				
				// Replace two lowest values
				if (val.getLow2() < lowest[0]) {
					lowest[0] = val.getLow1();
					lowest[1] = val.getLow2();
				
				// Replace only one value
				} else if (val.getLow1() < lowest[1]){
					lowest[1] = val.getLow1();
				}
			}
			
			average = (float)sum / (float)count;
			stockSummary.setLow1(lowest[0]);
			stockSummary.setLow2(lowest[1]);
			stockSummary.setSum(sum);
			stockSummary.setAverage(average);
			context.write(key, stockSummary);
		}
	}

	public static void main(String[] args) throws Exception {
//		args[0] = "/user/acdepand/cloudera/workspace/Stock/stock";
//		args[1] = "/user/acdepand/cloudera/workspace/Stock/output/output1.txt";
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StockPrice");
		job.setJarByClass(StockPrice.class);
		job.setMapperClass(StockMapper.class);
		job.setCombinerClass(StockCombiner.class);
		job.setReducerClass(StockReducer.class);
		// Specify Mapper Output class if different from OutputClass
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StockInfo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StockSummary.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		// Codes won't run with the line
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	// StockInfo is a tuple class that saves top2 low values, 
	// sum and count number of each stock in the tuple
	private static class StockInfo implements Writable {
		private int low1 = Integer.MAX_VALUE;
		private int low2 = Integer.MAX_VALUE;
		private int sum = 0;
		private int count = 0;

		public void setLow1(int val) {
			low1 = val;
		}
		
		public void setLow2(int val) {
			low2 = val;
		}
		
		public void setSum(int val) {
			sum = val;
		}
		
		public void setCount(int val) {
			count = val;
		}
	
		public int getLow1() {
			return low1;
		}
		
		public int getLow2() {
			return low2;
		}

		public int getSum() {
			return sum;
		}
		
		public int getCount() {
			return count;
		}
		
		public void readFields(DataInput in) throws IOException {
			low1 = in.readInt();
			low2 = in.readInt();
			sum = in.readInt();
			count = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(low1);
			out.writeInt(low2);
			out.writeInt(sum);
			out.writeInt(count);
			
		}
		
		@Override
		public String toString() {
			return "Lowest prices: " + low1 + ", " + low2 + 
					"\tSum: " + sum + "\tCount: " + count;
		}	
	}
	
	// StockSummary is a tuple class that stores 
	// top2 low values, sum and average of each stock in the tuple
	private static class StockSummary implements Writable {
		private int low1 = Integer.MAX_VALUE;
		private int low2 = Integer.MAX_VALUE;
		private int sum = 0;
		private float average = 0;

		public void setLow1(int val) {
			low1 = val;
		}
		
		public void setLow2(int val) {
			low2 = val;
		}
		
		public void setSum(int val) {
			sum = val;
		}
		
		public void setAverage(float val) {
			average = val;
		}
	
		public int getLow1() {
			return low1;
		}
		
		public int getLow2() {
			return low2;
		}

		public int getSum() {
			return sum;
		}
		
		public float getAverage() {
			return average;
		}
		
		public void readFields(DataInput in) throws IOException {
			low1 = in.readInt();
			low2 = in.readInt();
			sum = in.readInt();
			average = in.readFloat();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(low1);
			out.writeInt(low2);
			out.writeInt(sum);
			out.writeFloat(average);	
		}
		
		@Override
		public String toString() {
			return "Lowest prices: " + low1 + ", " + low2 + 
					"\tSum: " + sum + "\tAverage: " + average;
		}	
	}
}
