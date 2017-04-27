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

#include <sys/stat.h>

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
    
    typedef struct packet_struct
    {
        unsigned int   timestamp;
        int8_t    antenna;
        int8_t timestream[nsamples];
        char comp_timestamp[26];
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
    
    int            x;          /* Loop variable */
    int stop = 0;
    int nappends = 0;
    
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
    printf("Read buffer size %d\n", BUFSIZE);
    
    //Set up date information.
    gettimeofday(&curTime, NULL);
    micro = curTime.tv_usec;
    
    char buffer [80];

    strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", localtime(&curTime.tv_sec));
    
    char currentTime[26] = "";
    sprintf(currentTime, "%s.%06d", buffer, micro);
    printf("Current time: %s \n", currentTime);
    
    //create the output directory if it doesn't exist.
    sprintf(buffer, "dataOut/d%s/", dateBuffer);
    mkdir(buffer, ACCESSPERMS);
    
    sprintf(outfname, "dataOut/d%s/%s.config", dateBuffer, argv[2]);
    printf("Config file: %s\n", outfname);
    FILE *configHandler = fopen(outfname, "w+");
    fprintf(configHandler, "Number channels: %d\n", number_channels);
    fprintf(configHandler, "Number frames:  %d\n", number_frames);
    fprintf(configHandler, "Data start time: %s\n", currentTime);
    printf("Config create time: %s\n", currentTime);
    printf("Number channels: %d\n", number_channels);
    printf("Number frames:  %d\n", number_frames);
    
    
    
    printf("Beginning file write loop.\n");
    for (int loop_number = 0; loop_number < file_write_loops; ++loop_number){
        
        int lastPrint_sec = curTime.tv_sec;
        int print_timestamp = 0, timestamp_index = 0, new_timestamp_loop = 0;
        int tot_packets = number_frames*number_channels;
        
        strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", localtime(&curTime.tv_sec));
        sprintf(currentTime, "%s.%06d", buffer, micro);
        sprintf(outfname, "dataOut/d%s/%s.%04d.bin", dateBuffer, argv[2], loop_number);
        
        fprintf(configHandler, "%s\n", outfname);
        fprintf(configHandler, "File start time: %s\n", currentTime);
        
        printf("%s\n", outfname);
        printf("File start time: %s\n", currentTime);
        
        FILE *outDataHandler = fopen(outfname,"wb");
        fwrite(&tot_packets, sizeof(int), 1, outDataHandler);
        fwrite(&number_channels, sizeof(int), 1, outDataHandler);
        fwrite(&currentTime, sizeof(currentTime), 1, outDataHandler);
        
        printf("\nGo!\n");
        gettimeofday(&curTime, NULL);
        tic = clock();
        x=0;
        
        //start data acquisition
        while(x < tot_packets){
            
            signal(SIGINT, sig_handler);
            
            //recvfrom: receive a UDP datagram or read from file
            memset(buf, 0, BUFSIZE);
            n = recvfrom(sockfd, buf, BUFSIZE, 0,
                        (struct sockaddr *) &clientaddr, &clientlen);
            
            if (n < 0)
                error("ERROR in recvfrom");
            
            if(verbose)
                printf("number of bytes received: %d\n", n);
            
            frame_id = buf[0] & 0xF0;
            
            if ( frame_id == timestream_frame_id_value ) { //Got a timestream frame
                
                if(verbose)
                    printf("Got a packet.\n");
                
                
                //unpack the header
                ant_channel = buf[0] & 0x0F;
                stream_id = (uint16_t) (((uint16_t)buf[1]<<8) | (buf[2]));
                word_length = (uint16_t) (((uint16_t)buf[3]<<8) | (buf[4]));
                timestamp = (uint32_t) (((uint32_t)buf[5]<<24) | ((uint32_t)buf[6]<<16) | ((uint32_t)buf[7]<<8) | buf[8]);
                
                gettimeofday(&curTime, NULL);
                micro = curTime.tv_usec;
                strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", localtime(&curTime.tv_sec));
                sprintf(currentTime, "%s.%06d", buffer, micro);
                sprintf(singleTime.comp_timestamp, currentTime);
                
                //assing data to singleTime
                singleTime.timestamp = timestamp;
                singleTime.antenna = ant_channel;
                for (int i = 0; i < nsamples; ++i) {
                    singleTime.timestream[i] = buf[i+9];
                }
                
                
                //Write to disk and then clear the structure's timestream.
                fwrite(&singleTime, sizeof(singlePacket)-1, 1, outDataHandler);
                
                //Zero struct values
                for (int i = 0; i < nsamples; ++i) {
                    singleTime.timestream[i] = 0;
                }
                
                
                x+=1;
                
                if(verbose){
                    printf("current time: %s \n", currentTime);
                    printf("%hhX, %hX, %hX, %X \n", ant_channel, stream_id, word_length, timestamp);
                }
                
                if(curTime.tv_sec - lastPrint_sec>= 2){
                    printf("At: packet number %d/%d; file number %d/%d .\n", x, tot_packets, loop_number+1, file_write_loops);
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
                    if(timestamp_index == 32){
                        print_timestamp = 0;
                        new_timestamp_loop = 0;
                        printf("+++++++++++++++\n");
                    }
                    
                }
                
                
                
            }
            
            
            
        }
        

       fclose(outDataHandler);

    }

    printf("Cleaning up...");
    fclose(configHandler);
    printf("Done.\n");
    
}
