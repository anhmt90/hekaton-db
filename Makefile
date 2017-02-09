CXXFLAGS:=-g -std=c++11 -Wall -Wextra -pthread -Isrc -lrt -Wl,--no-as-needed,-E -ldl 

DEPTRACKING=-MD -MF $(@:.o=.d)
BUILD=/usr/bin/g++ -o$@ $(CXXFLAGS) $(LDFLAGS) $^

CHECKDIR=@mkdir -p $(dir $@)


#all: bin/main examples_bin
all: bin/main

#include hpp/LocalMakefile
#include cpp/LocalMakefile
#include operators/LocalMakefile

-include bin/*.d bin/*/*.d  

bin/%.o: src/%.cpp
	$(CHECKDIR)
	/usr/bin/g++ -o$@ -c $(CXXFLAGS) $(DEPTRACKING) $< 

#bin/operators/%.o: operators/%.cpp
#	$(CHECKDIR)
#	/usr/bin/g++ -o$@ -c $(CXXFLAGS) $(DEPTRACKING) $< 
	
	
obj:=bin/Attribute.o bin/Table.o bin/Schema.o bin/Types.o bin/Transaction.o

bin/main: bin/main.o $(obj)
	#g++ -std=c++11 -g -O3 -Wall -fPIC -rdynamic TranslatedQuery.cpp -shared -o TranslatedQuery.so
	$(BUILD)	
	
	
#bin/scanexample: bin/examples/ScanExample.o $(obj)
#	$(BUILD)
#
#bin/selectexample: bin/examples/SelectExample.o $(obj)
#	$(BUILD)
#
#bin/joinexample: bin/examples/JoinExample.o $(obj)
#	$(BUILD)
#	
#bin/chiexample: bin/examples/ChiExample.o $(obj)
#	$(BUILD)


# Alias for all examples
#examples_bin: bin/scanexample bin/selectexample bin/joinexample bin/chiexample

clean:
	find bin -name '*.d' -delete -o -name '*.o' -delete -o '(' -perm -u=x '!' -type d ')' -delete

	