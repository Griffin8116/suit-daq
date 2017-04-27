/*
 * suitcase interferometer UDP packet receiver
 * based on chime_receiver.c - A simple UDP chime receiver
 * originally started from http://www.cs.cmu.edu/afs/cs/academic/class/15213-f99/www/class26/udpserver.c
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include "hdf5.h"
#include "hdf5_hl.h"

#define BUFSIZE 32767
#define nsamples 2048

/*
 * error - wrapper for perror
 */
void error(char *msg) {
    perror(msg);
    exit(1);
}

void sig_handler(int sigNumber) {
    if (sigNumber == SIGINT) {
        printf("User interrupted, hopefully exiting nicely...\n");
        exit(0);
    }
}

int main(int argc, char **argv) {

    int ntimes = 3600;

    typedef struct visibility
    {
        char* comp_timestamp;
        unsigned int   timestamp;
        int8_t    antenna;
        int8_t timestream[nsamples];

    } singlePacket;

    singlePacket singleTime;


    int sockfd; /* socket */
    int portno; /* port to listen on */
    int clientlen; /* byte size of client's address */
    struct sockaddr_in serveraddr; /* server's addr */
    struct sockaddr_in clientaddr; /* client addr */
    struct hostent *hostp; /* client host info */
    uint8_t buf[BUFSIZE]; /* message buf */
    char *hostaddrp; /* dotted decimal host addr string */
    int optval; /* flag value for setsockopt */
    int n; /* message byte size */
    char frame_id;  /* frame id*/
    char ant_channel;
    char timestream_frame_id_value = 0xA0;
    unsigned short stream_id;
    unsigned short word_length;
    unsigned int timestamp;
    char fname[256];
    char outfname[256];
    float system_frame_period = 2.56e-06;
    int file_write_loops;


    ///hdf5 stuff
    hsize_t dims = nsamples;
    //hsize_t dims_flags = nfreq;

    hid_t          fid;        /* File identifier */
    hid_t          ptable;     /* Packet table identifier */
    //hid_t          ptable_flags;  /*  Packet table for flags */
    hid_t          my_dt;         /*  data type of row of data*/
    hid_t         dt_timestream, dt_comp_timestream;   /* timestream array data type */

    herr_t         err, status;        /* Function return status */
    hsize_t        count;      /* Number of records in the table */




    int            x;          /* Loop variable */
    int stop = 0;
    int nappends = 0;



    ////int64_t singleTime[ncorr][2*nfreq];
    ////uint8_t flags[ncorr][nfreq];
    int32_t first;
    clock_t tic;
    clock_t toc;
    clock_t total;

    char dateBuffer [80];
    struct timeval curTime;
    gettimeofday(&curTime, NULL);
    int micro = curTime.tv_usec;
    strftime(dateBuffer, 80, "%Y%m%d", localtime(&curTime.tv_sec));
    
    /*
     * check and use command line arguments
     */
    if (argc < 6 || argc > 7) {
        fprintf(stderr, "usage: %s <port> <outfile> <number files> <number frames> <number channels> [verbose = 1/0]\n", argv[0]);
        exit(1);
    }
    int verbose = 0;
    portno = atoi(argv[1]);
    file_write_loops = atoi(argv[3]);
    int number_frames = atoi(argv[4]);
    int number_channels = atoi(argv[5]);
    if(argc == 7)
        verbose = atoi(argv[6]);


    printf("File write loops: %d\n", file_write_loops);
    printf("Frames: %d\n", number_frames);
    printf("Number channels: %d\n", number_channels);
    /*
     * socket: create the parent socket
     */
    printf("Opening socket...\n");
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)
        error("ERROR opening socket");

    /* setsockopt: Handy debugging trick that lets
     * us rerun the server immediately after we kill it;
     * otherwise we have to wait about 20 secs.
     * Eliminates "ERROR on binding: Address already in use" error.
     */
    optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR,
               (const void *)&optval , sizeof(int));


    /*
     * build the server's Internet address
     */
    memset((char *) &serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons((unsigned short)portno);

    /*
     * bind: associate the parent socket with a port
     */
    printf("Binding socket...\n");
    if (bind(sockfd, (struct sockaddr *) &serveraddr,
             sizeof(serveraddr)) < 0)
        error("ERROR on binding");

    /*
     * main loop: wait for a datagram, sort and write to disk
     */
    clientlen = sizeof(clientaddr);
    printf("buffer size %d\n", BUFSIZE);

    printf("Beginning file write loop.\n");
    for (int loop_number = 0; loop_number < file_write_loops; ++loop_number)
    {
        //Initialize HDF5 file and datatypes
        dt_timestream      = H5Tarray_create2( H5T_NATIVE_CHAR, 1,  &dims );

        hid_t strtype = H5Tcopy (H5T_C_S1);
        status = H5Tset_size (strtype, H5T_VARIABLE);
    
        //Define a frame data type for H5
        my_dt = H5Tcreate(H5T_COMPOUND, sizeof(singlePacket));

        err = H5Tinsert(my_dt, "comp_timestamp", HOFFSET(singlePacket, comp_timestamp), strtype);
        err = H5Tinsert(my_dt, "timestamp",      HOFFSET(singlePacket, timestamp),      H5T_NATIVE_UINT);
        err = H5Tinsert(my_dt, "antenna",        HOFFSET(singlePacket, antenna),        H5T_NATIVE_UCHAR);
        err = H5Tinsert(my_dt, "timestream",     HOFFSET(singlePacket, timestream),     dt_timestream);
        

        hsize_t d = 1;
        hid_t attr_id, tot_packets_attr_id, date_attr_id; //attribute identifier
                  
        //dataspace_id for attributes
        hid_t dataspace_id = H5Screate(H5S_SCALAR);

        //Set up string attribute stuff for date attribute
        hid_t atype = H5Tcopy(H5T_C_S1);
        H5Tset_size(atype, 84);
        H5Tset_strpad(atype,H5T_STR_NULLTERM);

        //Set up date information.
        gettimeofday(&curTime, NULL);
        micro = curTime.tv_usec;

        char buffer [80];
        strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", localtime(&curTime.tv_sec));

        char currentTime[84] = "";
        sprintf(currentTime, "%s.%06d", buffer, micro);
        printf("current time: %s \n", currentTime);

        int lastPrint_sec = curTime.tv_sec;
        int print_timestamp = 0;
        int timestamp_index = 0;
        int new_timestamp_loop = 0;
        int tot_frames = number_frames*number_channels;


        sprintf(outfname, "dataOut/d%s/%s.%04d.h5", dateBuffer, argv[2], loop_number);
        printf("%s\n", outfname);
        fid=H5Fcreate(outfname,H5F_ACC_TRUNC,H5P_DEFAULT,H5P_DEFAULT);

        ptable  = H5PTcreate_fl(fid, "ADC_Timestream_Data", my_dt, (hsize_t)1, -1);

        attr_id = H5Acreate(fid, "Number_channels", H5T_NATIVE_INT, dataspace_id, H5P_DEFAULT, H5P_DEFAULT);
        tot_packets_attr_id = H5Acreate(fid, "Number_packets", H5T_NATIVE_INT, dataspace_id, H5P_DEFAULT, H5P_DEFAULT);
        date_attr_id = H5Acreate2(fid, "Date", atype, dataspace_id, H5P_DEFAULT, H5P_DEFAULT);

        status = H5Awrite(attr_id, H5T_NATIVE_INT, &number_channels);
        printf("Attribute number_channels write status: %d\n", status);
        status = H5Awrite(tot_packets_attr_id, H5T_NATIVE_INT, &tot_frames);
        printf("Attribute number_packets write status: %d\n", status);

        status = H5Awrite(date_attr_id, atype, currentTime);
        printf("Attribute date write status: %d\n", status);

        /*
        singleTime.comp_timestamp = "-1";       
        //assing data to singleTime
        singleTime.timestamp = 0;
        singleTime.antenna = 0;
        for (int i = 0; i < nsamples; ++i) {
            singleTime.timestream[i] = -125;
        }

        err = H5PTappend(ptable, (hsize_t)1, &(singleTime) );*/
        
        //sleep(1);
        //Clear some packets from the buffer.
        //for(int p = 0; p < 1000; p++){
        //    memset(buf, 0, BUFSIZE);
        //    n = recvfrom(sockfd, buf, BUFSIZE, 0,
        //                 (struct sockaddr *) &clientaddr, &clientlen);
        //}

        printf("\nGo!\n");
        gettimeofday(&curTime, NULL);
        tic = clock();
        x=0;
        
        while (x < (tot_frames)) {

            signal(SIGINT, sig_handler);
            /*
             * recvfrom: receive a UDP datagram or read from file
             */
            memset(buf, 0, BUFSIZE);

            n = recvfrom(sockfd, buf, BUFSIZE, 0,
                         (struct sockaddr *) &clientaddr, &clientlen);
            if (n < 0)
                error("ERROR in recvfrom");

            if(verbose)
                printf("number of bytes received: %d\n", n);



            frame_id = buf[0] & 0xF0;
            
            if ( frame_id == timestream_frame_id_value ) { /*  Got a timestream frame*/
                
                if(verbose)
                    printf("Got a frame.\n");


                /* unpack the header*/
                ant_channel = buf[0] & 0x0F;
                stream_id = (uint16_t) (((uint16_t)buf[1]<<8) | (buf[2]));
                word_length = (uint16_t) (((uint16_t)buf[3]<<8) | (buf[4]));
                timestamp = (uint32_t) (((uint32_t)buf[5]<<24) | ((uint32_t)buf[6]<<16) | ((uint32_t)buf[7]<<8) | buf[8]);
                
                gettimeofday(&curTime, NULL);
                micro = curTime.tv_usec;
                strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", localtime(&curTime.tv_sec));
                sprintf(currentTime, "%s.%06d", buffer, micro);
                //printf("current time: %s \n", currentTime);
                singleTime.comp_timestamp = currentTime;
                
                //assing data to singleTime
                singleTime.timestamp = timestamp;
                singleTime.antenna = ant_channel;
                for (int i = 0; i < nsamples; ++i) {
                    singleTime.timestream[i] = buf[i+9];
                }
                
                


                /* Write to disk if got a new timestamp and initialize array to max value. First timestamp most likely not full.*/
                err = H5PTappend(ptable, (hsize_t)1, &(singleTime) );


                //Zero struct values
                for (int i = 0; i < nsamples; ++i) {
                    singleTime.timestream[i] = 0;
                }


                x+=1;

                if(verbose)
                      printf("%hhX, %hX, %hX, %X \n", ant_channel, stream_id, word_length, timestamp);
                      //printf("%d %d\n", lastPrint_sec, curTime.tv_sec);
                    
                    
                if(curTime.tv_sec - lastPrint_sec>= 2){
                    printf("At: frame number %d/%d; file number %d/%d .\n", x, tot_frames, loop_number+1, file_write_loops);
                    printf("%s\n", currentTime);
                    print_timestamp = 1;
                    timestamp_index = 0;
                    lastPrint_sec = curTime.tv_sec;
                    printf("+++++++++++++++\n");
                }

                
                if(print_timestamp){
                    if(1){
                        new_timestamp_loop = 1;
                        
                    }
                    if(new_timestamp_loop){ //i.e. we've received channel 0's data
                        
                        printf("%3d, %X, %X, %X \n", timestamp_index, ant_channel, word_length, timestamp);
                        timestamp_index++;
                        //printf("-------------------\n");
                    }
                    if(timestamp_index == 16){
                        print_timestamp = 0;
                        new_timestamp_loop = 0;
                        printf("+++++++++++++++\n");
                    }

                }   

            }

    

        }
        printf("Writing & closing file...");
        err = H5Sclose(dataspace_id);
        err = H5Aclose(attr_id);
        err = H5Aclose(tot_packets_attr_id);
        err = H5Aclose(date_attr_id);

        err = H5PTclose(ptable);
        H5Fclose(fid);
        printf("Done.\n");
    }

}
