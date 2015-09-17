package com.anilkc.crystalball;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Anil
 *
 */
public class Pair implements Serializable, WritableComparable<Pair> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1700143742902690191L;

	private String firstValue;
	private String secondValue;

	public Pair(String firstValue, String secondValue) {
		super();
		this.firstValue = firstValue;
		this.secondValue = secondValue;
	}

	public Pair() {
		super();
	}

	public String getFirstValue() {
		return firstValue;
	}

	public void setFirstValue(String firstValue) {
		this.firstValue = firstValue;
	}

	public String getSecondValue() {
		return secondValue;
	}

	public void setSecondValue(String secondValue) {
		this.secondValue = secondValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstValue == null) ? 0 : firstValue.hashCode());
		result = prime * result + ((secondValue == null) ? 0 : secondValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (firstValue == null) {
			if (other.firstValue != null)
				return false;
		} else if (!firstValue.equals(other.firstValue))
			return false;
		if (secondValue == null) {
			if (other.secondValue != null)
				return false;
		} else if (!secondValue.equals(other.secondValue))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Pair (" + firstValue + ", " + secondValue + ")";
	}

	public int compareTo(Pair pair) {
		int firstResult = Integer.valueOf(this.getFirstValue()).compareTo(Integer.valueOf(pair.getFirstValue()));
		if (firstResult == 0) {
			if (this.getSecondValue().equals(CommonConstants.SPECIAL_CHARACTER)) {
				return -1;
			}
			if (pair.getSecondValue().equals(CommonConstants.SPECIAL_CHARACTER)) {
				return 1;
			}
			return Integer.valueOf(this.getSecondValue()).compareTo(Integer.valueOf(pair.getSecondValue()));
		} else {
			return firstResult;
		}
	}

	public void readFields(DataInput dataIp) throws IOException {
		firstValue = dataIp.readUTF();
		secondValue = dataIp.readUTF();
	}

	public void write(DataOutput dataOp) throws IOException {
		dataOp.writeUTF(firstValue.toString());
		dataOp.writeUTF(secondValue.toString());

	}

}
