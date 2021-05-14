package pt.sotubo.planner.solver;

import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

public class PurchaseStockItemCapacityTracker implements IStockItemCapacityTracker{

	@Override
	public void insertRequirement(StockItemTransaction resourceRequirement, WorkOrder wo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertProduction(StockItemTransaction resourceProduction, WorkOrder wo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void retractRequirement(StockItemTransaction resourceRequirement, WorkOrder wo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void retractProduction(StockItemTransaction resourceProduction, WorkOrder wo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getHardScore() {
		// TODO Auto-generated method stub
		return 0;
	}

}
