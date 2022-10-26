#Makefile
#Created by adaM(ldscfe@gmail.com) 2022-9-28

ORACLE_HOME=/home/bi/oralib/11.2.0.4

#Need Modify
EXECUTABLE=expo
SOURCES=expo.cpp

CC=g++ -pthread -std=c++11
DEBUG=-Drele
CFLAGS=-c -Wall $(DEBUG)
OBJECTS=$(SOURCES:.cpp=.o)

#Oracle OCI
LD_ORCL= -lclntsh
LIB_ORCL= -I${ORACLE_HOME}/rdbms/demo -I${ORACLE_HOME}/rdbms/public -L${ORACLE_HOME}/lib/

#UserDefine
LDFLAGS=${LD_ORCL}
LIBFLAGS= ${LIB_ORCL} -I/usr/include/udefcp


#Common
all: $(SOURCES) $(EXECUTABLE)
	
$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(LIBFLAGS) $(LDFLAGS) $(OBJECTS) -o $@

.cpp.o:
	$(CC) $(CFLAGS) $(LIBFLAGS) $< -o $@

clean:
	rm -rf *o $(EXECUTABLE)
