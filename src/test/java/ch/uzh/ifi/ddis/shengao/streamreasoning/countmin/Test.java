package ch.uzh.ifi.ddis.shengao.streamreasoning.countmin;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.sun.org.apache.bcel.internal.classfile.Field;

public class Test {

	public String[] readLineOfFile(String inputFile) {
		int lineCount = 0;
		
		BufferedReader br;
		String [] xs = null;
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
		int seed = 7364181;
		Random r = new Random(seed);
		String[] xs = readLineOfFile(inputFile);
		int numItems = xs.length;
		int absentItems = numItems * 10;
		
		int maxScale = 20;
		
		double epsOfTotalCount = 0.001;
		double confidence = 0.99;
		System.out.println("number of items: "+ numItems);
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
		System.out.println("error of wrong count: "+ numErrors);
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
		System.out.println("total error: "+ numErrors);
		
		double pCorrect = 1.0 - ((double) numErrors) / (numItems + absentItems);
		System.out.println(pCorrect);
		assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);

		assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
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
		myTest.testLUBMStrings("./data/uba-10/uni-012-copy-123.txt");

	}

}
