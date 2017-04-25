# pylint: disable=C0103, W1202, line-too-long

import os
import time
import sys
from datetime import datetime
#import multiprocessing
import getopt
import logging

import numpy as np
import correlator

import h5py

sys.path.append('~/work/code/h5view/')
import h5view as h5v
  
from frame import Frame

 
def accumulate_frame(handler, acc_dict, frame):
    '''
    Calculate the correlation products of a data frame and 
    add it to an accumulator. If the accumulator is full, 
    then write the data to disk.
    '''

    if acc_dict['filled'] == 0:
        acc_dict['timestamp'] = frame.packet_timestamps[0]
        acc_dict['index'] = frame.index
#        print frame.index
#        for i in xrange(4):
#            print frame.packet_timestamps[i]


    datum = correlator.convert_frame_multiple_channels(frame.frame_data)

    acc_dict['accumulator'] += correlator.return_triangle_array(correlator.correlate(datum))
    acc_dict['frame_indices'][acc_dict['filled']] = frame.index
    acc_dict['frame_numbers'][acc_dict['filled']] = frame.frame_number
    acc_dict['filled'] += 1

    if acc_dict['filled'] == acc_dict['n_acc']:
        write_to_disk(handler, acc_dict)
        acc_dict['write_index'] += 1
        acc_dict['accumulator'] *= 0
        acc_dict['frame_indices'] *= 0
        acc_dict['frame_numbers'] *= 0
        acc_dict['filled'] = 0



def write_to_disk(dataset_handler, acc_dict):
    '''
    Write a completed accumulation to disk.
    '''

    dataset_handler[acc_dict['write_index']] = (acc_dict['index'], 
                                                acc_dict['timestamp'], 
                                                acc_dict['accumulator'], 
                                                #acc_dict['frame_indices'],
                                                acc_dict['frame_numbers']
                                               )


def parse_raw_data(data_in_handle, data_out_handle, number_channels, to_parse=None, n_acc = 1000):
    '''
    Parse hdf5 file from packets into frames and accumulate them. 
    '''

    if to_parse is not None and to_parse > 0:
        total_packets = to_parse
    else:
        total_packets = data_in_handle.attrs['Number_packets']
    
    n_corr = number_channels * (number_channels + 1) / 2
    
    accumulator = {
        'accumulator': np.zeros((n_corr, 1024), dtype=np.complex64), 
        'frame_indices': np.zeros((n_acc, ), dtype=np.int),         
        'frame_numbers': np.zeros((n_acc, ), dtype=np.uint32),                 
        'filled': 0,
        'write_index': 0,
        'n_acc': n_acc,
        'timestamp': "",
        'index': -1
    }

    mailbox = []
    frame_index = 0
    need_new = True

    old_frames = []
    write_frames = []
   
    completed_frame_total = 0
    number_incomplete_frames = 0
    number_forgotten_packets = 0

    current_time = time.time()
    start_time = current_time
#    last_time = current_time
    
    cleaning_thresh = 1000
    DISPLAY_THRESH = 20000

    for packet_index in xrange(total_packets):

        current_time = time.time()
        packet = data_in_handle['ADC_Timestream_Data'][packet_index]

        #if current_time - last_time > 6:
        if len(mailbox) > 100 or (packet_index % DISPLAY_THRESH == 0 and packet_index != 0):    
            #last_time = current_time
            logger.info("-+-+-+-+-+-+-+-+-+-+-+-+-+-")
            run_time = (time.time() - start_time) / 60.
            logger.info("Run time: {0:.3f} minutes.".format(run_time))
            logger.info("Estimated time remaining: {0:.2f} minutes."\
            .format(total_packets / (packet_index / run_time) - run_time))
            logger.info("Packets analysed: {0:6g}/{1:6g}".format(packet_index, total_packets))
            logger.debug("Completed frames to date: {0:6g}".format(completed_frame_total))
            logger.info("Completed accumulations to date: {0:6g}".format(accumulator['write_index']+1))
            logger.debug("Packet-to-frame ratio: {0:.4f}".format(float(packet_index)/completed_frame_total))
            logger.debug("Number of incomplete frames: {0:d}".format(number_incomplete_frames))
            logger.debug("Number of forgotten packets: {0:d}".format(number_forgotten_packets))
            logger.info("Number of mailboxes: {0:d}".format(len(mailbox)))


            if len(mailbox) > 0 and frame_index - mailbox[0].index > cleaning_thresh:
                logger.info("Oldest mailbox has an index out of bounds: {0:d}:{1:d}."\
                .format(packet_index, mailbox[0].index))

                for mailbox_number, frame in enumerate(mailbox):
                    if frame_index - frame.index > cleaning_thresh:
                        old_frames += [mailbox_number]
                        logger.debug("Incomplete frame:")
                        logger.debug("\n\n" + str(frame) + "\n")
                        
                        number_forgotten_packets += frame.number_packets()
                
                logger.debug("Removed mailboxes: " + str(old_frames))
                
                if len(old_frames) > 0:
                    number_incomplete_frames += len(old_frames)
                    mailbox = [i for j, i in enumerate(mailbox) if j not in old_frames]
                    old_frames = []
                        

                logger.info("Updated number of mailboxes: {0:d}".format(len(mailbox)))

        for mailbox_number, frame in enumerate(mailbox):
            if frame.frame_number == packet['timestamp']:
                
                if frame.add_packet(packet): #Returns true if the packet is full, false otherwise.
                    write_frames += [mailbox_number]                        
                    accumulate_frame(data_out_handle, accumulator, frame)
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

                
    logger.info("++++++++++++++++++++++++++++++++++++++++")
    logger.info("At end of datafile; final results:")
    logger.info("Mailbox size: {0:d}\n".format(len(mailbox)))
    for mailbox_number, frame in enumerate(mailbox):
        if frame.is_full():
            #This should never happen.
            print "Complete frame:"
            print frame
            print "\n"
            completed_frame_total += 1
        else:
            logger.info("Incomplete frame:")
            logger.info("\n" + str(frame) + "\n")
            number_incomplete_frames += 1
            number_forgotten_packets += frame.number_packets()

    logger.info("++++++++++++++++++++++++++++++++++++++++")
    logger.info("Final completed frames: {0:d}".format(completed_frame_total))
    if completed_frame_total != 0:
        logger.info("Packet-to-frame ratio: {0:.4f}".format(float(total_packets)/completed_frame_total))
    else:
        logger.info("No completed frames!")
    logger.info("Number of incomplete frames: {0:d}".format(number_incomplete_frames))
    logger.info("Number of forgotten packets: {0:d}".format(number_forgotten_packets))
    logger.info("          Equivalent frames: {0:d}".format(int(np.ceil(number_forgotten_packets/float(number_channels)))))

    logger.info("Ideal frame total: {0:d}".format(completed_frame_total\
                                         + int(np.ceil(number_forgotten_packets/float(number_channels)))))

    logger.info("\n")
    return accumulator['write_index']

def usage():
    '''
    Usage function.
    '''

    print "Usage: {0:s} <filename>\n\
         \t -h/--help: This message.\n\
         \t -f/--file: Input file name.\n\
         \t -p/--packets: Number of packets to analyse.\n\
         \t -n/--nacc: Number of frames to accumulate.\n\
         \t -l/--log: Log level".format(sys.argv[0])


if __name__ == "__main__":


    INPUT_FILENAME = "EMPTY"
    N_TO_ANALYSE = None
    N_ACC = 100
    LOG_LEVEL = "INFO"

    if len(sys.argv) == 1:
        usage()
        sys.exit(2)

    try:
    #print sys.argv
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "hi:p:n:l:",
                                   ["help",
                                    "infile=", 
                                    "packets=", 
                                    "nacc=", 
                                    "log=", 
                                   ])
    #print opts
    #print args
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt, arg in OPTS:
        if opt in ("-h", "--help"):
            usage()          
            sys.exit(2)
        if opt in ("-i", "--infile"):
            INPUT_FILENAME = arg
        elif opt in ("-p", "--packets"):
            N_TO_ANALYSE = int(arg)
        elif opt in ("-l", "--log"):
            LOG_LEVEL = arg
        elif opt in ("-n", "--nacc"):
            N_ACC = int(arg)


    # create logger
    logger = logging.getLogger(__name__) # pylint: disable=locally-disabled, invalid-name

    numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % LOG_LEVEL)

    logger.setLevel(numeric_level)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(numeric_level)
    # create formatter
    
    #formatter = logging.Formatter("%(asctime)s - %(funcName)s - %(levelname)s - %(message)s")
    formatter = logging.Formatter("%(funcName)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)            



    START_TIME = time.time()
 #   INPUT_FILENAME = sys.argv[1]
 #   N_TO_ANALYSE = None
 #   N_ACC = 1000

#    if len(sys.argv) >= 3:
#        N_TO_ANALYSE = int(sys.argv[2])
#    if len(sys.argv) == 4:
#        N_ACC = int(sys.argv[3])

    #Open up the input file. 
    try:
        INPUT_FILE = h5py.File(INPUT_FILENAME, "r")
    except IOError:
        logger.critical("Error opening input file; exiting.")
        #INPUT_FILE.close()       
        sys.exit()

    logger.info("{0:s}\tFile size: {1:s}"\
        .format(INPUT_FILE, h5v.format_size(os.path.getsize(INPUT_FILENAME))))
    logger.info("Input file atributes: ")
    for k in INPUT_FILE.attrs.keys():
        logger.info("\t{0:s} : {1:s}".format(k, str(INPUT_FILE.attrs[k])))

    #Retrieve some constants
    N_CHANNELS = INPUT_FILE.attrs['Number_channels']     
    TOT_PACKETS = INPUT_FILE.attrs['Number_packets']
    MAX_FRAMES = int(TOT_PACKETS / 4) + 10 if int(TOT_PACKETS / 4) > 1000 else 1010
    #Compute number unique correlations
    NUMBER_CORRELATIONS = N_CHANNELS * (N_CHANNELS + 1) / 2

    logger.info("Total packets in file: {0:d}".format(TOT_PACKETS))
    logger.info("Maximum number frames: {0:d}".format(MAX_FRAMES))

    if N_TO_ANALYSE is not None:
        logger.info("Number of frames to analyse: {0:d}".format(N_TO_ANALYSE))

    logger.info("Number of frames per accumulation: {0:d}".format(N_ACC))
    logger.info("Logging level: {0:s}".format(LOG_LEVEL))

    #Set up and open output file
    SOURCE_FILENAME = os.path.split(INPUT_FILENAME)
    OUTPUT_FILENAME = SOURCE_FILENAME[0] + "/correlated/PC_" + SOURCE_FILENAME[1]
    
    if not os.path.exists(os.path.dirname(OUTPUT_FILENAME)):
        os.makedirs(os.path.dirname(OUTPUT_FILENAME))
    #try:
    #    OUTPUT_FILE = h5py.File(OUTPUT_FILENAME, 'w')    
    #except IOError:
    #    print "Out data file already open... Closing."
    #    OUTPUT_FILE.close()       

    if os.path.isfile(OUTPUT_FILENAME):
        logger.info("Target file '{0:s}' exists; overwriting.".format(OUTPUT_FILENAME))
        os.remove(OUTPUT_FILENAME)
        logger.info("Deleted '{0:s}'".format(OUTPUT_FILENAME))

    OUTPUT_FILE = h5py.File(OUTPUT_FILENAME, 'w')    
        
    OUTPUT_FILE.attrs['process_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    OUTPUT_FILE.attrs['number_accumulations'] = N_ACC
    
    #Assign attributes to new data file
    for k in INPUT_FILE.attrs.keys():
        OUTPUT_FILE.attrs[k] = INPUT_FILE.attrs[k] 

    OUTPUT_FILE.attrs['Truncated_analysis'] = bool(N_TO_ANALYSE is not None and N_TO_ANALYSE > 0)        

    COMP_TYPE = np.dtype([('index', 'int64'), 
                          ('timestamp', '|S30'), 
                          ('products', 'complex64', (NUMBER_CORRELATIONS, 1024)),
                          #('frame_indices', 'int32', (N_ACC, )),
                          ('frame_numbers', 'uint32', (N_ACC, )),                          
                         ])

    OUTPUT_HANDLER = OUTPUT_FILE.create_dataset("correlations", 
                                                (MAX_FRAMES,), 
                                                dtype=COMP_TYPE, 
                                                maxshape=(MAX_FRAMES,), 
                                                compression="gzip", 
                                                compression_opts=9)


    CORRELATION_TOTAL = parse_raw_data(INPUT_FILE, OUTPUT_HANDLER, 
                                       N_CHANNELS, N_TO_ANALYSE, N_ACC)


    OUTPUT_FILE.attrs['number_correlations'] = CORRELATION_TOTAL
    OUTPUT_FILE.attrs['true_number_entries'] = CORRELATION_TOTAL
    OUTPUT_HANDLER.resize((CORRELATION_TOTAL,))

    logger.info("Number of accumulations in output file: {0:d}".format(CORRELATION_TOTAL))
#    print "Printing first three entries: "
#    for i in xrange(3):
#        print "Frame index: {0:6g}\tFrame timestamp: {1}".format(dataset_handler[i]['index'], 
#            np.uint32(dataset_handler[i]['frame_number']))
    #print OUTPUT_FILE
    INPUT_FILE.close()    
    OUTPUT_FILE.close()
    logger.info("----------------------------------------")
    logger.info("Run time: {0:.3f}".format(time.time() - START_TIME))
    logger.info("Number accumulations: {0:d}".format(CORRELATION_TOTAL))
