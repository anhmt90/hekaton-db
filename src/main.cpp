/*
 * main.cpp
 *
 *  Created on: Jan 19, 2017
 *      Author: anhmt90
 */
#include <iostream>
#include <memory>
#include <algorithm>
#include <thread>
//#include <climits>
//#include <signal.h>
//#include <unistd.h>
//#include <sys/types.h>
//#include <sys/wait.h>
//#include <atomic>
//#include <dlfcn.h>

using namespace std;

#include "Schema.hpp"
#include "Transaction.hpp"

void run(int i){
	std::this_thread::sleep_for(std::chrono::seconds(1));
	Transaction* T = new Transaction(i);
}

int main(int argc, char* argv[]) {
	warehouse = *(new Warehouse("warehouse"));
	for(auto &w : warehouse.pk_index){
		cout << w.second.begin << " " << w.second.end << " | " << w.second.w_id <<"\t" << w.second.w_street_1 << "\n";
	}

//	run(3);

//	for(int i=0; i<2; ++i){
//		run(i);
//	}

//	thread T1(run,1);
//	thread T2(run,2);

	thread T1(run,2);
	std::this_thread::sleep_for(std::chrono::milliseconds(2500));
	thread T2(run,1);

	T1.join();
	T2.join();
}


