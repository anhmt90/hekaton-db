/*
 * main.cpp
 *
 *  Created on: Jan 19, 2017
 *      Author: anhmt90
 */
#include <iostream>
#include <memory>
#include <algorithm>
//#include <climits>
//#include <signal.h>
//#include <unistd.h>
//#include <sys/types.h>
//#include <sys/wait.h>
//#include <atomic>
//#include <dlfcn.h>


#include "Schema.hpp"
#include "Transaction.hpp"

//Warehouse_Table warehouse;
//OrderLine_Table orderline;


//Global, monotonically increasing counter
//extern uint64_t GMI_cnt = 0;

int main(int argc, char* argv[]) {
//	TPCC* tpcc = new TPCC();
	warehouse = *(new Warehouse_Table("warehouse"));
	for(auto &w : warehouse.pk_index){
		cout << w.second.begin << " " << w.second.end << " | " << w.second.w_id <<"\t" << w.second.w_street_1 << "\n";
	}

	Transaction* T = new Transaction();
//	orderline = *(new OrderLine_Table("orderline"));
}


