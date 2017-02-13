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


def correlate(fourier):
    '''
    Correlate values 
    
    params:
        fourier: list of FFTs (one for each channel)
        
    returns a 3D correlation triangle with NaNs in the lower-triangle components.
    '''    
    
    f_shape = fourier.shape 
    #returns something that looks like (N, N_frequency_channels)
    #N = f_shape[0] #This is the number of channels that we are cross-correlating.
    #N_frequency_channels = f_shape[1] #this is the number of frequency bins
    
    #Create correlation triangle with the right shape. Thus, it is a triangle with indices 
    # i,j (i, j <= N) and k == N_frequency_channels
    
    corr_triangle = np.empty((f_shape[0], f_shape[0], f_shape[1]), dtype=np.complex128)
    
    for i, fft in enumerate(fourier):
        for j, fft2 in enumerate(fourier):                        
            corr_triangle[j,i] = np.complex128(fft*np.conjugate(fft2)) if i >= j else np.complex128() * np.nan
    return corr_triangle

        
def return_triangle_array(corr_triangle):
    '''
    Return array corresponding to upper triangle.
    '''
    return corr_triangle[np.triu_indices(corr_triangle.shape[0])]


def convert_frame_multiple_channels(data):
    '''
    Convert IceBoard frame data into complex scaler values.
    '''

    return data[:, ::2] + 1.0*1j*data[:, 1::2]

def now_str():
    '''
    Return a string containing the current time.
    '''
    return datetime.now().strftime("%H:%M:%S.%f")


def main():
    #if len(sys.argv) != 2:
    #    print "Error; need input file name."
    #    return

    #start_time = time.time()
    #filename = sys.argv[1] 
    data_file = "/home/sean/work/cosmology/suit/DAQ/suit-daq/testing/test_code/test_data/processed/pr_test_stream.h5"

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
 #                         ('frame_number', 'uint32'),
 #                         ('packet_timestamps', '|S30', number_channels), 
                          ('correlation_products', 'complex128', 
                           (number_correlations, number_frequencies))
                         ])
    


    #comp_type = np.dtype([('correlation_products', 'complex128', 
    #                       (number_correlations, number_frequencies))])
    dataset_handler = out_data.create_dataset("correlated_timestream", 
                                              (number_entries,), 
                                              dtype=comp_type, 
                                              maxshape=(number_entries,), 
                                              compression="gzip", compression_opts=9)


    gains = np.zeros((number_channels, number_frequencies), dtype=np.complex128) + 1. #placeholder

    print "Start  {0:s}".format(now_str())

    frame_timestream = in_data['frame_timestream']

    for i in xrange(number_entries):
        sliced_data = frame_timestream[i]
        datum = convert_frame_multiple_channels(sliced_data['frame_data'])*gains
        correlations = correlate(datum)

        dataset_handler[i] = (sliced_data['index'], 
        #                      sliced_data['frame_number'], 
        #                      sliced_data['packet_timestamps'],
                              return_triangle_array(correlations))
        #print dataset_handler[i].dtype
        #print return_triangle_array(correlations).dtype
        #print return_triangle_array(correlations).shape
        #print np.array(return_triangle_array(correlations), dtype=comp_type).dtype
        #dataset_handler[i] = np.array(return_triangle_array(correlations), dtype=comp_type)

        if i > 0 and i%100 == 0:
            print "At {0:d}/{1:d}".format(i, number_entries)

    print "Finish {0:s}".format(now_str())

if __name__ == "__main__":
    main()
