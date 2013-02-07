import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAFTopNPercent extends UDAF{
	
	public static class Result {
		ArrayList<Double> list;
		double min;
	}
	
	public class TopNPercentEvaluator implements UDAFEvaluator {
		
		private Result res;
		private int rowIndex;
		private int percent;
		
		public TopNPercentEvaluator() {
			super();
			res = new Result();
			init();
			rowIndex = 0;
		}
		@Override
		public void init() {
			res.list = new ArrayList<Double>();
			res.min = Double.MAX_VALUE;
		}
		
		public boolean iterate(Double rowVal, int pct) {
			ArrayList<Double> resList = res.list;
			rowIndex++;
			resList.add(rowVal);
			percent = pct;
			return true;
		}
		
		public ArrayList<Double> terminatePartial() {
			ArrayList<Double> resList = res.list;
			Collections.sort(resList);
			return resList;
		}
		
		public boolean merge(ArrayList<Double> otherList) {
			ArrayList<Double> resList = res.list;
			resList.addAll(otherList);
			return true;
		}
		
		public ArrayList<Double> terminate() {
			ArrayList<Double> resList = res.list;
			double num_rows = (double)percent/100.0*rowIndex;
			Collections.sort(resList);
			int lastIdx = resList.size()- (int) num_rows;
			if(lastIdx <= 0) {
				return resList;
			}
			for(int i=0; i<lastIdx; i++) {
				resList.remove(i);
			}
			return resList;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
