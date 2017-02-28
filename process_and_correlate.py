import os
import time
import sys
from datetime import datetime
#import multiprocessing
#sys.path.append('~/work/cosmology/code/h5view/')

import numpy as np

import h5view as h5v
import h5py

from correlator import *

  
    
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
        self.packet_timestamps = ['' for string in xrange(num_channels)] #the timestamps for each packet
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

                if self.is_full():
                    return True
                else:
                    return False

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

def write_frame(dataset_handler, datum, write_index):
    '''
    Write a completed frame to disk.
    '''

    dataset_handler[write_index] = datum


def parse_and_write_data(data_in_handle, data_out_handle, number_channels, to_parse=None):
    '''
    Parse hdf5 file from packets into frames.
    '''
    total_packets = data_in_handle.attrs['Number_packets']
    
    mailbox = []
    frame_index = 0
    need_new = True

    old_frames = []
    write_frames = []
    

    completed_frame_total = 0 #acts as an index for frame writing; also acts as an attribute later.
    number_incomplete_frames = 0
    number_forgotten_packets = 0

    current_time = time.time()
    start_time = current_time
    last_time = current_time
    #data = data_in_handle['ADC_Timestream_Data']

    cleaning_threshold = 100

    #for packet_index, packet in enumerate(data_in_handle):
    if to_parse is not None:
        total_packets = to_parse

    for packet_index in xrange(total_packets):

        if to_parse is not None and packet_index >= to_parse:
            break

        packet = data_in_handle['ADC_Timestream_Data'][packet_index]

        current_time = time.time()
        #if (packet_index > 0) and (packet_index % 5000 == 0):
        if current_time - last_time > 6:
            last_time = current_time
            print "-+-+-+-+-+-+-+-+-+-+-+-+-+-"
            run_time = (time.time() - start_time) / 60.
            print "Run time: {0:.3f} minutes.".format(run_time)
            print "Estimated time remaining: {0:.2f} minutes.".format(total_packets / (packet_index / run_time) - run_time)
            print "Frames analysed: {0:6g}/{1:6g}".format(packet_index, total_packets)
            print "Completed frames to date: {0:6g}".format(completed_frame_total)
            print "Packet-to-frame ratio: {0:.4f}".format(float(packet_index)/completed_frame_total)
            print "Number of incomplete frames: {0:d}".format(number_incomplete_frames)
            print "Number of forgotten packets: {0:d}".format(number_forgotten_packets)
            print "Number of mailboxes: {0:d}".format(len(mailbox))


            if len(mailbox) > 0 and frame_index - mailbox[0].index > cleaning_threshold:
                print "Oldest mailbox has an index out of bounds: {0:d}:{1:d}.".format(packet_index, mailbox[0].index)

                for mailbox_number, frame in enumerate(mailbox):
                    if frame_index - frame.index > cleaning_threshold:
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
                #print "Matching {0:d}, ch{1:d} to frame with index {2:d}".format(
                #    packet['timestamp'], packet['antenna'], mailbox_number)
                
                if frame.add_packet(packet): #Returns true if the packet is full, false otherwise.
                    write_frames += [mailbox_number]     
                    
                    write_frame(data_out_handle, frame.array(), completed_frame_total)
                        #print "Writing."
                    #completed_frames[completed_frame_total] = frame.frame_number
                    completed_frame_total += 1 
                    #print 'Frame complete: {0:d}'.format(frame.frame_number)
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
    print "At end of datafile; writing remaining frames:\n"
    print "Mailbox size: {0:d}\n".format(len(mailbox))
    for mailbox_number, frame in enumerate(mailbox):
        if frame.is_full():
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

def main():
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        print "Usage: {0:s} <filename> [number packets to analyse] [number of integrations] ".format(sys.argv[0])
        return

    start_time = time.time()

    filename = sys.argv[1] #"/home/sean/work/cosmology/ice/ch_acq/chrx/v1/dataOut/messy.0000"
    
    number_to_analyse = None

    if len(sys.argv) >= 3:
        number_to_analyse = int(sys.argv[2])
	if len(sys.argv) == 4:
        n_accumulations = int(sys.argv[3])

    try:
        in_data = h5py.File(filename, "r")
    except IOError:
        print "In data file already open... Closing."
        in_data.close()       
    print "{0:s}\tFile size: {1:s}".format(in_data, h5v.format_size(os.path.getsize(filename)))

    print "Atributes: " 
    for k in in_data.attrs.keys():
        print "\t{0:s} : {1:s}".format(k, str(in_data.attrs[k]))


    number_channels = in_data.attrs['Number_channels']    
    datestr = in_data.attrs['Date']
    file_date = datestr[0:10]
    file_time = datestr[11::]

    print "Date taken: {0:s}".format(datestr)
   
    total_packets = in_data.attrs['Number_packets']
    print "Total packets in file: {0:d}".format(total_packets)

    max_num_frames = int(total_packets / 4) + 10 if int(total_packets / 4) > 1000 else 1010
    print "Maximum number frames: ", max_num_frames

    if number_to_analyse is not None:
        print "Number of frames to analyse: {0:d}".format(number_to_analyse)


    path_split = os.path.split(filename)  
    #processed_filename = "pdata/processed_" + path_split[1]
    processed_filename = path_split[0] + "/processed/pr_" + path_split[1]
    if not os.path.exists(os.path.dirname(processed_filename)):
        os.makedirs(os.path.dirname(processed_filename))
    try:
        out_data = h5py.File(processed_filename, 'w')    
    except IOError:
        print "Out data file already open... Closing."
        out_data.close()
        
        
    out_data.attrs['process_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    for k in in_data.attrs.keys():
        out_data.attrs[k] = in_data.attrs[k] 

    if number_to_analyse is not None:
        out_data.attrs['Truncated_analysis'] = True
    else:
        out_data.attrs['Truncated_analysis'] = False


    #comp_type = Frame(number_channels).comp_type
    comp_type = np.dtype([('index', 'int64'), 
                          ('frame_number', 'uint32'),
                          ('packet_timestamps', '|S30', number_channels), 
                          ('frame_data', 'int8', (number_channels, 2048))])


    dataset_handler = out_data.create_dataset("frame_timestream", 
                                              (max_num_frames-10,), 
                                              dtype=comp_type, 
                                              maxshape=(max_num_frames,), 
                                              compression="gzip", compression_opts=9)


    final_entries = parse_and_write_data(in_data, dataset_handler, 
                                         number_channels, number_to_analyse)


    out_data.attrs['true_number_entries'] = final_entries
    dataset_handler.resize((final_entries,))
    #print dataset_handler.dtype
    print "Final number of entries in file: {0:d}".format(final_entries)
    print "Printing first three entries: "
    for i in xrange(3):
        #print "Frame index: {0:d} Frame timestamp: {1:u} Packet timestamps: {2:s}"\
        #.format(dataset_handler[i]['index'], np.uint32(dataset_handler[i]['frame_number']), dataset_handler[i]['packet_timestamps'])
        print "Frame index: {0:6g}\tFrame timestamp: {1}".format(dataset_handler[i]['index'], 
            np.uint32(dataset_handler[i]['frame_number']))

    in_data.close()    
    out_data.close()
    elapsed_time = time.time() - start_time
    print "----------------------------------------"

    print elapsed_time
    print final_entries


if __name__ == "__main__":
    main()
