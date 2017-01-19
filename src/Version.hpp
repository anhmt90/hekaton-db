/*
 * Version.hpp
 *
 *  Created on: Dec 20, 2016
 *      Author: anhmt90
 */

class Version {
	double begin;
	double end;
	Version* next;
};

class Warehouse_Version : public Version {
	Warehouse_Row payload;

};
