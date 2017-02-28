import os
import time
import sys
from datetime import datetime

#import multiprocessing
#sys.path.append('~/work/cosmology/code/h5view/')

import numpy as np
#import matplotlib.pyplot as pl

import h5view as h5v
import h5py

from correlator import *

def now_str():
    '''
    Return a string containing the current time.
    '''
    return datetime.now().strftime("%H:%M:%S.%f")

def write_slice(dataset_handler, datum, write_index):
    '''
    Write a completed accumulation to disk.
    '''

    dataset_handler[write_index] = datum    

def return_slice(dataset_handler, index):
    '''
    Read a slice from an h5py file handler.
    '''
    return dataset_handler[index]

def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print "Usage: {0:s} <filename> [number integrations == 100]"
        return

    

    data_file = sys.argv[1] #"/home/sean/work/cosmology/ice/ch_acq/chrx/v1/dataOut/messy.0000"
    
    n_acc = 100

    if len(sys.argv) == 3:
        n_acc = int(sys.argv[2])



    #start_time = time.time()
    

    in_data = h5py.File(data_file, "r")
    print "{0:s}\n\tFile size: {1:s}\n".format(in_data, h5v.format_size(os.path.getsize(data_file)))




    print "Atributes: " 
    for k in in_data.attrs.keys():
        print "\t{0:s} : {1:s}".format(k, str(in_data.attrs[k]))

    
    path_split = os.path.split(data_file)  
    correlated_filename = path_split[0] + "/cr_" + path_split[1][3::]

    if os.path.isfile(correlated_filename):
        os.remove(correlated_filename)
        print "Deleted '{0:s}'".format(correlated_filename)

    out_data = h5py.File(correlated_filename, 'a')    
    print "Opened '{0:s}' for writing...".format(correlated_filename)

    out_data.attrs['correlation_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    for k in in_data.attrs.keys():
        out_data.attrs[k] = in_data.attrs[k] 



    number_frequencies = 1024
    number_channels = in_data.attrs['Number_channels']    
    number_entries = in_data.attrs['true_number_entries']    
    number_correlations = number_channels * (number_channels + 1) / 2
 

    comp_type = np.dtype([('index', 'int64'), 
                          ('timestamp', '|S30'), 
                          ('products', 'complex64', (number_correlations, number_frequencies))
                          ])
    dataset_handler = out_data.create_dataset("correlations", 
                                              (number_entries,), 
                                              dtype=comp_type, 
                                              maxshape=(number_entries,), 
                                              compression="gzip", compression_opts=9)

    gains = np.ones((number_channels, number_frequencies), dtype=int)
    

    print "Start  {0:s}".format(now_str())

    frame_timestream = in_data['frame_timestream']

    accumulator = return_triangle_array(correlate(convert_frame_multiple_channels(return_slice(frame_timestream, 0)['frame_data'])))
    
    acc_index = 0
    #n_acc = int(1000) 
    out_data.attrs['accumulations_per_measurement'] = n_acc


    accumulator = accumulator*0.
    write_index = 0

    for i in xrange(number_entries):
        sliced_data = frame_timestream[i]

        if acc_index == 0:
            frame_index = sliced_data['index']
            timestamp = sliced_data['packet_timestamps'][0]
            #print frame_index, timestamp

        datum = convert_frame_multiple_channels(sliced_data['frame_data'])*gains

        accumulator += return_triangle_array(correlate(datum))

        if acc_index == n_acc -1:

            #print "Writing accumulation to disk."
            dataset_handler[write_index] = (frame_index, timestamp, accumulator)

            write_index += 1

            acc_index = 0
            accumulator = 0.*accumulator
            
        else:
            acc_index += 1

        if i > 0 and i%1000 == 0:
            print "[{2:s}] At {0:d}/{1:d}".format(i, number_entries, now_str())
            print "\tCurrent packet information: {0:5d}; {1:30s}".format(frame_index, timestamp)

    dataset_handler.resize((write_index,))
    out_data.attrs['true_number_entries'] = write_index
    out_data.close()
    print "\nFinished {0:s}".format(now_str())

    print "\n\n"
    print "-------"
    data_file = h5v.File(correlated_filename)
    print data_file


    data_file.close()

if __name__ == "__main__":
    main()
