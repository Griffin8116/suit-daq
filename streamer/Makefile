CC=gcc
HDF5ROOT=/home/sean/work/code/hdf5
LIBS = -L $(HDF5ROOT)/lib -lhdf5 -lhdf5_hl
INC  = -I $(HDF5ROOT)/include/

suit_timestream_receiver: suit_timestream_receiver.c
	$(CC) suit_timestream_receiver.c -o suit_timestream_receiver $(INC) $(LIBS) 

binary: suit_binary_receiver.c
	$(CC) suit_binary_receiver.c -o suit_binary_receiver $(INC) $(LIBS) 