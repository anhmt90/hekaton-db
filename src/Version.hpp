/*
 * Version.hpp
 *
 *  Created on: Dec 20, 2016
 *      Author: anhmt90
 */

class Version {
	uint64_t begin;
	uint64_t end;
	Version* next;

	bool isGarbage = false;
};

class Warehouse_Version : public Version {
	Warehouse_Tuple payload;

	Warehouse_Version(Warehouse_Tuple* payload): payload(*payload)
	{ }
};

