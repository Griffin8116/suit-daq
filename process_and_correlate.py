import os
import time
import sys
from datetime import datetime
#import multiprocessing



import numpy as np
from correlator import *

import h5py

sys.path.append('~/work/code/h5view/')
import h5view as h5v
  
    
class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class Frame(object):

    '''
    Frame class to combine multiple packets into a single frame.
    '''
        
    def __init__(self, num_channels, packet, index=None):
        
        self.num_channels = num_channels #number channels 
        self.frame_data = np.zeros((num_channels, 2048)) #frame payload
        self.trace_received = np.zeros(num_channels) #flags for which packets are in frame
        self.frame_number = 0 #Frame number from FPGA (i.e. frame timestamp 32 bit number)
        #the timestamps for each packet
        self.packet_timestamps = ['' for string in xrange(num_channels)] 
        self.index = 0 #frame indexing number

        self.comp_type = np.dtype([('index', 'int64'), 
                                   ('frame_number', 'uint32'),
                                   ('packet_timestamps', '|S30', self.num_channels), 
                                   ('frame_data', 'int8', (self.num_channels, 2048))])


    #if packet is not None:
        self.frame_number = packet['timestamp']
        self.frame_data[int(packet['antenna'])] = packet['timestream']
        self.packet_timestamps[int(packet['antenna'])] = packet['comp_timestamp']
        self.trace_received[int(packet['antenna'])] = 1
    #if index is not None:
        self.index = index
    
    def __str__(self):

        printstr = "Frame index: {0:d}\n".format(self.index)
        printstr += "Frame number: {0:d}\n".format(self.frame_number)        
        printstr += "\tTotal number traces: {0:d}\n".format(self.num_channels)
        printstr += "\tTotal filled traces: {0:d}\n".format(int(np.sum(self.trace_received)))
        trace_key = ""
        for i in self.trace_received:
            trace_key += str(int(i)) 
        printstr += "\tReceived traces: {0:s}".format(trace_key)
        return printstr
        
    def is_full(self):
        '''
        Check if a frame has been filled.
        '''
        return int(sum(self.trace_received)) == self.num_channels

    def number_packets(self):
        '''
		Return the number of packets in a frame.
    	'''
        return int(sum(self.trace_received))

    def add_packet(self, packet):
        '''
        Add information in a packet to a frame.
        '''

        try:
            if self.frame_number != packet['timestamp']:
                raise Error('Mismatched timestamp on add_packet.')
            else:
                self.frame_data[int(packet['antenna'])] = packet['timestream']
                self.packet_timestamps[int(packet['antenna'])] = packet['comp_timestamp']
                self.trace_received[int(packet['antenna'])] = 1

                return self.is_full()

        except Error:
            raise


    def array(self):
        '''
        Return the compound structure array representation of the Frame object.

        '''

        return np.array([(self.index, 
                          self.frame_number, 
                          self.packet_timestamps, 
                          self.frame_data)], dtype=self.comp_type)



def accumulate_frame(handler, acc_dict, frame):
    '''
    Calculate the correlation products of a data frame and 
    add it to an accumulator. If the accumulator is full, 
    then write the data to disk.
    '''

    #return_triangle_array(correlate(convert_frame_multiple_channels(return_slice(frame_timestream, 0)['frame_data'])))
    datum = convert_frame_multiple_channels(frame.frame_data)
    
    acc_dict['accumulator'] += return_triangle_array(correlate(datum))
    acc_dict['filled'] += 1

    if acc_dict['filled'] == act_dict['n_acc']:
        write_to_disk(handler, acc_dict['accumulator'], acc_dict['write_index'])
        acc_dict['write_index'] += 1
        acc_dict['accumulator'] *= 0
        acc_dict['filled'] = 0



def write_to_disk(dataset_handler, datum, write_index):
    '''
    Write a completed accumulation to disk.
    '''

    dataset_handler[write_index] = datum


def parse_raw_data(data_in_handle, data_out_handle, number_channels, to_parse=None):
    '''
    Parse hdf5 file from packets into frames and accumulate them. 
    '''

    if to_parse is not None:
        TOT_PACKETS = to_parse
    else:
        TOT_PACKETS = data_in_handle.attrs['Number_packets']
    
    mailbox = []
    frame_index = 0
    need_new = True

    old_frames = []
    write_frames = []
   
    completed_accumulations = 0
    number_incomplete_frames = 0
    number_forgotten_packets = 0

    current_time = time.time()
    start_time = current_time
    last_time = current_time
    
    CLEANING_THRESH = 100

    for packet_index in xrange(TOT_PACKETS):

        current_time = time.time()
        packet = data_in_handle['ADC_Timestream_Data'][packet_index]

        if current_time - last_time > 6:
            last_time = current_time
            print "-+-+-+-+-+-+-+-+-+-+-+-+-+-"
            run_time = (time.time() - start_time) / 60.
            print "Run time: {0:.3f} minutes.".format(run_time)
            print "Estimated time remaining: {0:.2f} minutes."\
            .format(total_packets / (packet_index / run_time) - run_time)
            print "Frames analysed: {0:6g}/{1:6g}".format(packet_index, TOT_PACKETS)
            print "Completed frames to date: {0:6g}".format(completed_frame_total)
            print "Packet-to-frame ratio: {0:.4f}".format(float(packet_index)/completed_frame_total)
            print "Number of incomplete frames: {0:d}".format(number_incomplete_frames)
            print "Number of forgotten packets: {0:d}".format(number_forgotten_packets)
            print "Number of mailboxes: {0:d}".format(len(mailbox))


            if len(mailbox) > 0 and frame_index - mailbox[0].index > CLEANING_THRESH:
                print "Oldest mailbox has an index out of bounds: {0:d}:{1:d}."\
                .format(packet_index, mailbox[0].index)

                for mailbox_number, frame in enumerate(mailbox):
                    if frame_index - frame.index > CLEANING_THRESH:
                        old_frames += [mailbox_number]
                        print "Incomplete frame:"
                        print frame
                        print "\n"
                        number_forgotten_packets += frame.number_packets()
                
                print old_frames
                
                if len(old_frames) > 0:
                    number_incomplete_frames += len(old_frames)
                    mailbox = [i for j, i in enumerate(mailbox) if j not in old_frames]
                    old_frames = []
                        

                print "Updated number of mailboxes: {0:d}\n".format(len(mailbox))

        for mailbox_number, frame in enumerate(mailbox):
            if frame.frame_number == packet['timestamp']:
                
                if frame.add_packet(packet): #Returns true if the packet is full, false otherwise.
                    write_frames += [mailbox_number]                        
                    write_frame(data_out_handle, accumulator, frame, completed_accumulations)
                    completed_frame_total += 1 

                    mailbox = [i for j, i in enumerate(mailbox) if j not in write_frames]
                    write_frames = []

                break
            elif mailbox_number == len(mailbox) -1:
                #No mailboxes have matching frames. 
                need_new = True
                
        if need_new:
            new_frame = Frame(number_channels, packet, frame_index)
            frame_index += 1
            mailbox += [new_frame]
            need_new = False
        elif len(mailbox) == 0:
            need_new = True

                
    print "++++++++++++++++++++++++++++++++++++++++"        
    print "At end of datafile; final results:\n"
    print "Mailbox size: {0:d}\n".format(len(mailbox))
    for mailbox_number, frame in enumerate(mailbox):
        if frame.is_full():
            #This should never happen.
            print "Complete frame:"
            print frame
            print "\n"
            write_frame(data_out_handle, frame.array(), completed_frame_total)
            completed_frame_total += 1
        else:
            print "Incomplete frame:"
            print frame
            print "\n"
            number_incomplete_frames += 1
            number_forgotten_packets += frame.number_packets()

    print "++++++++++++++++++++++++++++++++++++++++"
    print "Final completed frames: {0:d}".format(completed_frame_total)
    if completed_frame_total != 0:
        print "Packet-to-frame ratio: {0:.4f}".format(float(total_packets)/completed_frame_total)
    else:
        print "No completed frames!"
    print "Number of incomplete frames: {0:d}".format(number_incomplete_frames)
    print "Number of forgotten packets: {0:d}".format(number_forgotten_packets)
    print "          Equivalent frames: {0:d}".format(number_forgotten_packets/number_channels)

    print "Ideal frame total: {0:d}".format(completed_frame_total\
                                         + number_forgotten_packets/number_channels)

    print "\n\n"
    return completed_frame_total


if __name__ == "__main__":

    if len(sys.argv) < 2 or len(sys.argv) > 4:
        print "Usage: {0:s} <filename>\
         [number packets to analyse]\
         [number of integrations] ".format(sys.argv[0])
        exit()

    START_TIME = time.time()
    INPUT_FILENAME = sys.argv[1] #"/home/sean/work/cosmology/ice/ch_acq/chrx/v1/dataOut/messy.0000"   
    N_TO_ANALYSE = None
    N_ACC = 1000

    if len(sys.argv) >= 3:
        N_TO_ANALYSE = int(sys.argv[2])
    if len(sys.argv) == 4:
        N_ACC = int(sys.argv[3])

    try:
        INPUT_DATA_HANDLE = h5py.File(INPUT_FILENAME, "r")
    except IOError:
        print "In data file already open... Closing."
        INPUT_DATA_HANDLE.close()       


    print "{0:s}\tFile size: {1:s}".format(in_data, 
                                           h5v.format_size(os.path.getsize(INPUT_FILENAME)))
    print "Input file atributes: " 
    for k in INPUT_DATA_HANDLE.attrs.keys():
        print "\t{0:s} : {1:s}".format(k, str(INPUT_DATA_HANDLE.attrs[k]))

    N_CHANNELS  = INPUT_DATA_HANDLE.attrs['Number_channels']     
    TOT_PACKETS = INPUT_DATA_HANDLE.attrs['Number_packets']
    MAX_FRAMES  = int(TOT_PACKETS / 4) + 10 if int(TOT_PACKETS / 4) > 1000 else 1010

    print "Total packets in file: {0:d}".format(TOT_PACKETS)
    print "Maximum number frames: {0:d}".format(MAX_FRAMES)

    if N_TO_ANALYSE is not None:
        print "Number of frames to analyse: {0:d}".format(N_TO_ANALYSE)

    print "Number of frames per accumulation: {0:d}".format(N_ACC)

    SOURCE_FILENAME = os.path.split(filename)
    OUTPUT_FILENAME = SOURCE_FILENAME[0] + "/correlated/PC_" + SOURCE_FILENAME[1]
    
    if not os.path.exists(os.path.dirname(OUTPUT_FILENAME)):
        os.makedirs(os.path.dirname(OUTPUT_FILENAME))
    try:
        OUTPUT_FILE = h5py.File(OUTPUT_FILENAME, 'w')    
    except IOError:
        print "Out data file already open... Closing."
        OUTPUT_FILE.close()       
        
    OUTPUT_FILE.attrs['process_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    OUTPUT_FILE.attrs['number_accumulations'] = N_ACC
    
    #assign attributes to new data file
    for k in in_data.attrs.keys():
        OUTPUT_FILE.attrs[k] = in_data.attrs[k] 

    OUTPUT_FILE.attrs['Truncated_analysis'] = bool(N_TO_ANALYSE is not None)        

    NUMBER_CORRELATIONS = number_channels * (number_channels + 1) / 2

    COMP_TYPE = np.dtype([('index', 'int64'), 
                          ('timestamp', '|S30'), 
                          ('products', 'complex64', (NUMBER_CORRELATIONS, 1024))
                         ])

    OUTPUT_HANDLER = out_data.create_dataset("correlations", 
                                             (max_num_frames,), 
                                             dtype=COMP_TYPE, 
                                             maxshape=(max_num_frames,), 
                                             compression="gzip", compression_opts=9)


    CORRELATION_TOTAL = parse_and_write_data(in_data, OUTPUT_HANDLER, 
                                             number_channels, number_to_analyse)


    OUTPUT_HANDLER.attrs['number_correlations'] = CORRELATION_TOTAL
    dataset_handler.resize((CORRELATION_TOTAL,))

    print "Number of accumulations in output file: {0:d}".format(CORRELATION_TOTAL)
#    print "Printing first three entries: "
#    for i in xrange(3):
#        print "Frame index: {0:6g}\tFrame timestamp: {1}".format(dataset_handler[i]['index'], 
#            np.uint32(dataset_handler[i]['frame_number']))

    in_data.close()    
    out_data.close()
    print "----------------------------------------"

    print time.time() - START_TIME
    print final_entries
