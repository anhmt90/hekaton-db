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

int main(int argc, char* argv[]) {
	vector<size_t> loops = {1000000};
	for(size_t i = 0, size = loops.size(); i < size; ++i){
		double total = orderline.pk_index.size() * loops[i];

		cout << endl;
		cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n";
		cout << "Updating the table OrderLine " << loops[i] << " times ... \n";

		auto start=high_resolution_clock::now();
		for(size_t z = 0; z < loops[i]; ++z)
			Transaction* T = new Transaction(z);
		auto end = duration_cast<duration<double>>(high_resolution_clock::now()-start).count();

		cout << "Accomplished in " << end << "s\n";
		cout << "Speed: " << total/(double)end << " updates/sec\n";

		cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n\n";
	}
}


