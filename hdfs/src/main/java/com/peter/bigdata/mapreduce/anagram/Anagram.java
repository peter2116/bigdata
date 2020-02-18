package com.peter.bigdata.mapreduce.anagram;

import com.peter.bigdata.hdfs.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/15
 * @Description: 字谜mapreduce
 * @modifier
 * 获取文件中相同字母组成的单词
 * eg: tag gat bath thab mapper pperam marpp
 * reust:
 * atg: tag,gat
 * abht: bath,thab
 * aeppr: mapper,pperram,marpp
 */
public class Anagram {

	public static class AnagramMapper extends Mapper<Object,Text,Text,Text> {
		private Text sortedText = new Text();
		@Override
		protected void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String word = value.toString();
			char[] wordChars = word.toCharArray();
			Arrays.sort(wordChars);
			String sortedWord = new String(wordChars);
			sortedText.set(sortedWord);
			context.write(sortedText,value);
		}
	}

	public static class AnagramReducer extends Reducer<Text,Text,Text,Text>{
		private Text outputVal = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String output = "";
			for(Text item : values) {
				if(!"".equals(output)) {
					output += ",";
				}
				output += item.toString();
			}
			StringTokenizer outputSt = new StringTokenizer(output,",");
			if(outputSt.countTokens() >= 2) {
				outputVal.set(output);
				context.write(key,outputVal);
			}
		}
	}

	/**
	 * mr任务运行配置
	 * @param args
	 */
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Path resutlPath = new Path("hdfs://cluster-100:9000/bigdata/anagram/output");
		FileSystem hadoopFs = FileSystemUtil.getHdfsFileSystem();
		if(hadoopFs.exists(resutlPath)) {
			hadoopFs.delete(resutlPath,true);
		}
		Job job = Job.getInstance();
		job.setJarByClass(Anagram.class);
		job.setMapperClass(AnagramMapper.class);
		job.setReducerClass(AnagramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,
				new Path("hdfs://cluster-100:9000/bigdata/anagram/input"));
		FileOutputFormat.setOutputPath(job,resutlPath);
		job.waitForCompletion(true);
	}

}
