package com.peter.bigdata.mapreduce.anagram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/15
 * @Description:
 * @modifier
 */
public class AnagramTest {

	private MapDriver mapDriver;

	private ReduceDriver reduceDriver;

	private MapReduceDriver mapReduceDriver;

	@Before
	public void setUp(){
		Mapper mapper = new Anagram.AnagramMapper();
		mapDriver = new MapDriver(mapper);
		Reducer reducer = new Anagram.AnagramReducer();
		reduceDriver = new ReduceDriver(reducer);
		mapReduceDriver = new MapReduceDriver(mapper,reducer);
	}

	/**
	 * test for map
	 * @throws IOException
	 */
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(),new Text("asdf"));
		mapDriver.withOutput(new Text("adfs"),new Text("asdf"));
		mapDriver.runTest();
	}


	/**
	 * test for reducer
	 */
	@Test
	public void testReducer() throws IOException {
		Text key = new Text("abc");
		List values = new ArrayList();
		values.add(new Text("cba"));
		values.add(new Text("bca"));
		reduceDriver.withInput(key,values)
				.withOutput(key,new Text("cba,bca"))
				.runTest();
	}

	/**
	 * test map reduce
	 */
	@Test
	public void testMr() throws IOException {
		String line1 = "abcd";
		String line2 = "acbd";
		mapReduceDriver.withInput(new LongWritable(),new Text(line1))
				.withInput(new LongWritable(),new Text(line2))
				.withOutput(new Text("abcd"),new Text("abcd,acbd"))
				.runTest();
	}

}
