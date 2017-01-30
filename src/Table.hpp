#ifndef H_Table_HPP
#define H_Table_HPP
//---------------------------------------------------------------------------

#include <fstream>
#include <unordered_map>
#include <string>
#include <vector>

#include "Attribute.hpp"
#include "Types.hpp"

//---------------------------------------------------------------------------
/// A database table
using namespace std;

//bool sortByType	(const Attribute& lhs, const Attribute& rhs){
//	return lhs.type < rhs.type;
//}
//bool sortByType (const Attribute& lhs, const Attribute& rhs);

class Table
{
public:

   /// Table name
   string name;
   /// List of attributes
   vector<Attribute> attributes;
   /// List of primary keys
   std::vector<unsigned> primaryKey;
   /// Count of rows
   size_t size;
   /// Constructor
   Table(const string name):name(name){};
   /// Destructor
   ~Table(){};

//   virtual insert()
   bool operator==(const Table& table) const{
	   return (name == table.name) && (attributes == table.attributes) && (size == table.size) && (primaryKey == table.primaryKey);
   }
   bool operator!=(const Table& table) const{
	   return !((name == table.name) && (attributes == table.attributes) && (size == table.size) && (primaryKey == table.primaryKey));
   }

};
//---------------------------------------------------------------------------
#endif
