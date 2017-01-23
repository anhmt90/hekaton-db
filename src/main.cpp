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

//Global, monotonically increasing counter
//extern uint64_t GMI_cnt = 0;

int main(int argc, char* argv[]) {
	TPCC* tpcc = new TPCC();
	for(auto &w : tpcc->warehouse){
		cout << w.second.begin << " " << w.second.end << " | " << w.second.w_id <<"\t" << w.second.w_street_1 << "\n";
	}

}


