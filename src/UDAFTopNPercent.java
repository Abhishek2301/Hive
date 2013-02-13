import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class UDAFTopNPercent extends UDAF {

	public static class Result {
		List<Double> list;
		int numRows;
		int pct;
	}

	public static class TopNPercentEvaluator implements UDAFEvaluator {

		private Result res;

		public TopNPercentEvaluator() {
			super();
			res = new Result();
			init();
		}

		@Override
		public void init() {
			res.list = new ArrayList<Double>();
			res.numRows = 0;
			res.pct = 0;
		}

		public boolean iterate(double rowVal, int pct) {
			List<Double> resList = res.list;
			res.numRows++;
			resList.add(rowVal);
			res.pct = pct;
			return true;
		}

		public Result terminatePartial() {
			if (res.list == null) {
				return null;
			}
			if (res.list.size() == 0) {
				return null;
			}
			List<Double> resList = res.list;
			Collections.sort(resList);
			List<DoubleWritable> resultList = new ArrayList<DoubleWritable>();
			for (int i = 0; i < resList.size(); i++) {
				resultList.add(i, new DoubleWritable(resList.get(i)));
			}
			return res;
		}

		public boolean merge(Result otherResult) {
			List<Double> resList = res.list;
			for (int i = 0; i < otherResult.list.size(); i++) {
				resList.add(otherResult.list.get(i));
			}
			res.numRows += otherResult.numRows;
			res.pct = Math.max(res.pct, otherResult.pct);
			return true;
		}

		public List<DoubleWritable> terminate() {
			List<Double> resList = res.list;
			List<DoubleWritable> resultList = new ArrayList<DoubleWritable>();
			int rowIndex = res.numRows;
			int percent = res.pct;
			double num_rows = (double) percent / 100.0 * rowIndex;
			Collections.sort(resList);
			Collections.reverse(resList);
			int lastIdx = resList.size() - (int) num_rows;
			if (lastIdx <= 0) {
				for (int i = 0; i < resList.size(); i++) {
					resultList.add(i, new DoubleWritable(resList.get(i)));
				}
				return resultList;
			}
			for (int i = 0; i < lastIdx; i++) {
				resList.remove(resList.size() - 1);
			}
			for (int i = 0; i < resList.size(); i++) {
				resultList.add(i, new DoubleWritable(resList.get(i)));
			}
			return resultList;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
