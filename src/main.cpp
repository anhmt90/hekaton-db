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
	std::this_thread::sleep_for(std::chrono::seconds(0));
	Transaction* T = new Transaction(i);
}

int main(int argc, char* argv[]) {
//	warehouse = *(new Warehouse());
//	for(auto &w : warehouse.pk_index){
//		cout << w.second.begin << " " << w.second.end << " | " << w.second.w_id <<"\t" << w.second.w_street_1 << "\n";
//	}

//	run(3);

//	for(int i=0; i<2; ++i){
//		run(i);
//	}

//	thread T1(run,1);
//	thread T2(run,2);

//	thread T1(run,2);
//	std::this_thread::sleep_for(std::chrono::milliseconds(2500));
//	thread T2(run,1);
//
//	T1.join();
//	T2.join();



//	orderline = *(new OrderLine());
//	district = *(new District());
//	uint64_t c = 0;
//	cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n\n";
//	auto start=high_resolution_clock::now();
//
//	for(size_t i = 0; i<1000t; i++){
//		for(auto V : orderline.pk_index){
//			++c;
//		}
//	}
//	auto end = duration_cast<duration<double>>(high_resolution_clock::now()-start).count();
//	cout << "Scanned " << c << " records in " << end << "s\n";
//	cout << "Speed: " << c/(double)end << " lookups/s\n";
	cout << endl;
	cout << "Order Buckets: " << order.pk_index.bucket_count() << " buckets\n";
	cout << "NewOrder Buckets: " << neworder.pk_index.bucket_count() << " buckets\n";
	cout << "OrderLine Buckets: " << orderline.pk_index.bucket_count() << " buckets\n";

	cout << "Order: " << order.pk_index.size() << " tuples\n";
	cout << "NewOrder: " << neworder.pk_index.size() << " tuples\n";
	cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n";

	size_t times = 1000000;
	cout << "Executing "<< times<< " transactions ... \n";
	auto start=high_resolution_clock::now();
	for(int z = 0; z < times; ++z)
		Transaction* T = new Transaction(z);
//	cout << "finished newOrder transaction " << iteration << " times in " << duration_cast<duration<double>>(high_resolution_clock::now()-start).count() << "s" << endl;

	auto end = duration_cast<duration<double>>(high_resolution_clock::now()-start).count();
	cout << "Transactions executing accomplished in " << end << "s\n";
	cout << "Speed: " << times/(double)end << " transactions/s\n";

	cout << "Order: " << order.pk_index.size() << " tuples\n";
	cout << "NewOrder: " << neworder.pk_index.size() << " tuples\n";
	cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n";
}


