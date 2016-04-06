package ch.uzh.ifi.ddis.shengao.streamreasoning.countmin;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

public class Test {
	int seed = 7364181;
	Random r = new Random(seed);
	int maxScale = 20;
	double epsOfTotalCount = 0.0001;
	double confidence = 0.999;
	CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
	Map<String, Long> actualFreq;
	int numItems = 0;
	int absentItems = numItems * 10;

	public String[] readLineOfFile(String inputFile) {
		int lineCount = 0;
		BufferedReader br;
		String[] xs = null;
		try {
			br = new BufferedReader(new FileReader(inputFile));
			String x = null;
			while ((x = br.readLine()) != null) {
				lineCount++;
			}
			br.close();
			br = new BufferedReader(new FileReader(inputFile));
			xs = new String[lineCount];
			int i = 0;
			while ((x = br.readLine()) != null) {
				xs[i] = x;
				i++;
			}
			br.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return xs;
	}

	public void testLUBMStrings(String inputFile) {
		String[] xs = readLineOfFile(inputFile);
		numItems = xs.length;
		absentItems = numItems * 10;

		System.out.println("number of items: " + numItems);
		sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
		for (String x : xs) {
			sketch.add(x, 1);
		}

		actualFreq = new HashMap<String, Long>();
		for (String x : xs) {
			Long val = actualFreq.get(x);
			if (val == null) {
				actualFreq.put(x, 1L);
			} else {
				actualFreq.put(x, val + 1L);
			}
		}
		testAccuracy();
	}

	public void testLUBMStringsWithDel(String inputFile) {
		
		String[] xs = readLineOfFile(inputFile);
		numItems = xs.length;
		absentItems = numItems * 10;
		
		System.out.println("number of items: "+ numItems);
		List<Boolean> isDelete = new LinkedList<Boolean>();
		List<String> trace = generateTrace(xs, 0.2, isDelete);
		actualFreq = new HashMap<String, Long>();
		
		int numInsert = 0;
		for (int i = 0; i< trace.size(); i++) {
			Long val = actualFreq.get(trace.get(i));
			if (val == null) {
				actualFreq.put(trace.get(i), 1L);
			} else {
				if (isDelete.get(i)) {
					if(val - 1L < 0){
						continue;
					}
					actualFreq.put(trace.get(i), val - 1L);
				}
				else
					actualFreq.put(trace.get(i), val + 1L);
			}
			
			if (isDelete.get(i)) {
				sketch.delete(trace.get(i), 1);
				
//				System.out.println("----------------");
//				System.out.println("round: "+i);
//				System.out.println("numInsert: "+numInsert);
//				System.out.println("numDelete: "+ (i - numInsert));
//				testAccuracy();
			}
			else{
				sketch.add(trace.get(i), 1);
				numInsert++;
			}
			
			
			if(i%1000 == 999 ) {
				System.out.println("----------------");
				System.out.println("round: "+i);
				System.out.println("numInsert: "+numInsert);
				System.out.println("numDelete: "+ (i - numInsert));
				testAccuracy();
			}
		}
		
		
		
	}

	public void testAccuracy() {
		sketch = CountMinSketch.deserialize(CountMinSketch.serialize(sketch));

		int numErrors = 0;
		for (Map.Entry<String, Long> entry : actualFreq.entrySet()) {
			String key = entry.getKey();
			long count = entry.getValue();
			double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
			long estimate = sketch.estimateCount(key);
			if (estimate > count) {
				numErrors++;
			} else if (estimate < count) {
				System.out.println("should never underestimate");
			}
			
//			if (ratio > epsOfTotalCount) {
//				numErrors++;
//			}
		}
		System.out.println("error of wrong count: " + numErrors );
		for (int i = 0; i < absentItems; i++) {
			int scale = r.nextInt(maxScale);
			String key = RandomStringUtils.random(scale);
			Long value = actualFreq.get(key);
			long count = (value == null) ? 0L : value;
			double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
			if (ratio > epsOfTotalCount) {
				numErrors++;
			}
		}
		System.out.println("total error: " + numErrors);

		double pCorrect = 1.0 - ((double) numErrors) / (numItems + absentItems);
		System.out.println(pCorrect);
		assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);

		assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
	}

	public List<String> generateTrace(String[] inputLines, double delRatio, List<Boolean> isDelete) {
		int numberToDelete = (int) Math.ceil((double) inputLines.length * delRatio);
		
		List<String> dataArray = new ArrayList<String>();
		
		int bucketSize = inputLines.length / numberToDelete;
		
		for (int i = 0; i < inputLines.length; i++) {
			dataArray.add(inputLines[i]);
			isDelete.add(false);
			if (i % bucketSize == bucketSize-1) {
				int toDelete = r.nextInt(bucketSize);
				dataArray.add(dataArray.get( i  - toDelete));
				isDelete.add(true);
			}
		}
		return dataArray;
	}

	public void testAccuracyStrings() {
		int seed = 7364181;
		Random r = new Random(seed);
		int numItems = 1000000;
		int absentItems = numItems * 10;
		String[] xs = new String[numItems];
		int maxScale = 20;
		for (int i = 0; i < numItems; i++) {
			int scale = r.nextInt(maxScale);
			xs[i] = RandomStringUtils.random(scale);
		}

		double epsOfTotalCount = 0.0001;
		double confidence = 0.99;

		CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
		for (String x : xs) {
			sketch.add(x, 1);
		}

		Map<String, Long> actualFreq = new HashMap<String, Long>();
		for (String x : xs) {
			Long val = actualFreq.get(x);
			if (val == null) {
				actualFreq.put(x, 1L);
			} else {
				actualFreq.put(x, val + 1L);
			}
		}

		sketch = CountMinSketch.deserialize(CountMinSketch.serialize(sketch));

		int numErrors = 0;
		for (Map.Entry<String, Long> entry : actualFreq.entrySet()) {
			String key = entry.getKey();
			long count = entry.getValue();
			double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
			if (ratio > epsOfTotalCount) {
				numErrors++;
			}
		}
		for (int i = 0; i < absentItems; i++) {
			int scale = r.nextInt(maxScale);
			String key = RandomStringUtils.random(scale);
			Long value = actualFreq.get(key);
			long count = (value == null) ? 0L : value;
			double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
			if (ratio > epsOfTotalCount) {
				numErrors++;
			}
		}

		double pCorrect = 1.0 - ((double) numErrors) / (numItems + absentItems);
		System.out.println(pCorrect);
		assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);

		assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Test myTest = new Test();
		//myTest.testLUBMStrings("./data/uba-10/uni-012-copy-123.txt");
		myTest.testLUBMStringsWithDel("./data/uba-10/uni-012-copy-123.txt");

	}

}
