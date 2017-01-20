/*
 * Version.hpp
 *
 *  Created on: Dec 20, 2016
 *      Author: anhmt90
 */

#ifndef H_VERSION
#define H_VERSION

#include "Schema.hpp"

struct Warehouse_Tuple;
struct OrderLine_Tuple;

class Version {
	uint64_t begin;
	uint64_t end;
	Version* next;

	bool isGarbage = false;
};

class Warehouse_Version : public Version {
	Warehouse_Tuple* payload;
public:
	Warehouse_Version(Warehouse_Tuple* payload) : payload(payload)
		{ }
};

class OrderLine_Version : public Version {
	OrderLine_Tuple* payload;
public:
	OrderLine_Version(OrderLine_Tuple* payload) : payload(payload)
		{ }
};


#endif
