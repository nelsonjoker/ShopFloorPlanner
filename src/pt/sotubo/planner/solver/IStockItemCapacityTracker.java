package com.joker.planner.solver;

import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;

public interface IStockItemCapacityTracker {

	void insertRequirement(StockItemTransaction resourceRequirement, WorkOrder wo);

	void insertProduction(StockItemTransaction resourceProduction, WorkOrder wo);

	void retractRequirement(StockItemTransaction resourceRequirement, WorkOrder wo);

	void retractProduction(StockItemTransaction resourceProduction, WorkOrder wo);

	int getHardScore();

}