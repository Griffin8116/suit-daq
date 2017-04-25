import numpy as np

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
        self.packet_timestamps = np.empty(num_channels, dtype=str)
        #['' for string in xrange(num_channels)] 
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
        '''
        Create the string representation of a Frame object.
        '''

        printstr = "Frame index: {0:d}\n".format(self.index)
        printstr += "Frame number: {0:X}\n".format(self.frame_number)        
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
        