package pt.sotubo.planner.solver;

import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

public interface IStockItemCapacityTracker {

	void insertRequirement(StockItemTransaction resourceRequirement, WorkOrder wo);

	void insertProduction(StockItemTransaction resourceProduction, WorkOrder wo);

	void retractRequirement(StockItemTransaction resourceRequirement, WorkOrder wo);

	void retractProduction(StockItemTransaction resourceProduction, WorkOrder wo);

	int getHardScore();
	int getSoftScore();

}