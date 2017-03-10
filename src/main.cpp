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
	if(i==2)
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	Transaction* T = new Transaction(i);
}

int main() {

	thread T1(run,1);
	thread T2(run,2);

	T1.join();
	T2.join();


//	cout << endl;
//	cout << "Order: " << order.pk_index.size() << " tuples\n";
//	cout << "NewOrder: " << neworder.pk_index.size() << " tuples\n";
//	cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n";
//
//	size_t times = 1000000;
//	cout << "Executing "<< times<< " transactions ... \n";
//	auto start=high_resolution_clock::now();
//	for(size_t z = 0; z < times; ++z)
//		Transaction* T = new Transaction(z);
////	cout << "finished newOrder transaction " << iteration << " times in " << duration_cast<duration<double>>(high_resolution_clock::now()-start).count() << "s" << endl;
//
//	auto end = duration_cast<duration<double>>(high_resolution_clock::now()-start).count();
//	cout << "Transactions executing accomplished in " << end << "s\n";
//	cout << "Speed: " << times/(double)end << " transactions/s\n";
//
//	cout << "Order: " << order.pk_index.size() << " tuples\n";
//	cout << "NewOrder: " << neworder.pk_index.size() << " tuples\n";
//	cout << "OrderLine: " << orderline.pk_index.size() << " tuples\n";
}


