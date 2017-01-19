/*
 * Attribute.hpp
 *
 *  Created on: Nov 18, 2016
 *      Author: anhmt90
 */
#ifndef H_Attribute
#define H_Attribute

#include <string>
#include <vector>
#include <regex>
#include <stdlib.h>
#include "Types.hpp"

using namespace std;



struct Attribute{
	string name;
	string type;
	string tablename;
	bool notNull;
	Attribute(): notNull(true){	}
	Attribute(string name):name(name),notNull(true){ }

	Attribute(string name, string type, string tablename):
		name(name), type(type), tablename(tablename), notNull(true){ }
	~Attribute(){ };

	bool operator< (const Attribute& attr) const {
		 if(type != attr.type )
			 return type < attr.type;
		 return  name < attr.name;
	}

	bool operator==(const Attribute& attr) const{
		return (name == attr.name) && (type == attr.type) && (tablename == attr.tablename);
	}
	bool operator!=(const Attribute& attr) const{
		return !((name == attr.name) && (type == attr.type) && (tablename == attr.tablename));
	}

	string toString(){
		return name+", "+type+", "+tablename+"\n";
	}
};

#endif
