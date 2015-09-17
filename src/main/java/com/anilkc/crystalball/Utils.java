package com.anilkc.crystalball;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Contains all the utility methods of this project
 * 
 * @author Anil
 *
 */
public class Utils {

	private Utils() {

	}

	private static final DecimalFormat df = new DecimalFormat("#.0000");

	public static double getFormattedDouble(double d) {
		return Double.valueOf(df.format(d));
	}

	public static String convertMapWritableToString(MapWritable mapWritable) {
		StringBuilder text = new StringBuilder();
		Iterator<Entry<Writable, Writable>> iterator = mapWritable.entrySet().iterator();
		Entry<Writable, Writable> out;

		while (iterator.hasNext()) {
			out = iterator.next();
			text.append("(");
			text.append(out.getKey());
			text.append(", ");
			text.append(out.getValue());
			text.append(")");

			if (iterator.hasNext()) {
				text.append(", ");
			}
		}
		return text.toString();
	}

	public static String converMaptoString(Map<String, Double> map) {
		StringBuilder text = new StringBuilder();
		Iterator<Entry<String, Double>> iterator = map.entrySet().iterator();
		Entry<String, Double> out;

		while (iterator.hasNext()) {
			out = iterator.next();
			text.append("(");
			text.append(out.getKey().toString());
			text.append(", ");
			text.append(out.getValue().toString());
			text.append(")");

			if (iterator.hasNext()) {
				text.append(", ");
			}
		}
		return text.toString();
	}
}
