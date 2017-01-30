################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/Attribute.cpp \
../src/Schema.cpp \
../src/Table.cpp \
../src/Transaction.cpp \
../src/Types.cpp \
../src/main.cpp 

OBJS += \
./src/Attribute.o \
./src/Schema.o \
./src/Table.o \
./src/Transaction.o \
./src/Types.o \
./src/main.o 

CPP_DEPS += \
./src/Attribute.d \
./src/Schema.d \
./src/Table.d \
./src/Transaction.d \
./src/Types.d \
./src/main.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


