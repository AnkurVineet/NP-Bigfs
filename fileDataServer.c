/***************************************\ 
*       NP Assignment 2 - Part 1        *
*       Ankur Vineet (2018H1030144P)    *
*       Aman Sharma  (2018H1030137P)    *
\***************************************/
//Data Server

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<fcntl.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<netinet/tcp.h>
#include<sys/signal.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<errno.h>

#define BUF_LEN 512
#define BLOCKSIZE 1000
#define FSBLOCK 1000000
#define MAXEVENTS 1000
#define READ 5
#define START 6
#define STORE 7
#define RM_CMD 8
#define RM_PROC 9
#define GETNAME 10
#define SENDFILE 11


struct event
{
	int fd;
	int state;
	char fname[100];
	int offset;
};

struct rm_msg
{
        int status;
        char msg[50];
};

void _mkdir(const char *dir) {
        char tmp[256];
        char *p = NULL;
        size_t len;
        snprintf(tmp, sizeof(tmp),"%s",dir);
        len = strlen(tmp);
	while(tmp[len - 1] != '/')
		len--;
        if(tmp[len - 1] == '/')
                tmp[len - 1] = 0;
        for(p = tmp + 1; *p; p++)
                if(*p == '/') {
                        *p = 0;
                        mkdir(tmp, S_IRWXU);
                        *p = '/';
                }
        mkdir(tmp, S_IRWXU);
}

int processcommand(char *command)
{
        if(strncmp(command, "st", 2) == 0)
                return START;
        if(strncmp(command, "rm", 2) == 0)
                return RM_CMD;
	if(strncmp(command, "sn", 2) == 0)
		return GETNAME;
        return -1;
}

int main(int argc, char **argv)
{
	fd_set masterfd,readfd,writefd,masterwfd;
	int port = 8090;
	if(argc == 2)
		port = atoi(argv[1]);
	printf("%d %d\n",argc,port);
        int lfd,connfd,flags,maxfd,acceptfd,nb,maxind,front=0,back=0;
        char buf[FD_SETSIZE][BUF_LEN+1];
	int client[FD_SETSIZE];
	struct event queue[MAXEVENTS];
	/*
	for(int q = 0; q < MAXEVENTS; q++)
		printf("q %d Fd: %d State: %d Name:%s Offset %d\n",q,queue[q].fd,queue[q].state,queue[q].fname,queue[q].offset);
        */
	struct sockaddr_in serveraddr;
	struct timeval tv;
        lfd = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&serveraddr, sizeof(serveraddr));
        serveraddr.sin_family = AF_INET;
        serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
        serveraddr.sin_port = htons(port);
        if(bind(lfd,(struct sockaddr *)&serveraddr,sizeof(serveraddr)) == -1)
                perror("bind: ");
        listen(lfd,10);
        if ( (flags = fcntl (lfd, F_GETFL, 0)) < 0)
                perror("F_GETFL: ");
        flags |= O_NONBLOCK;
        if (fcntl(lfd, F_SETFL, flags) < 0)
                perror("F_SETFL: ");
	FD_ZERO(&masterwfd);
	FD_ZERO(&masterfd);
	FD_ZERO(&writefd);
        FD_SET(lfd,&masterfd);
        maxfd = lfd;
	for( int i = 0 ; i < FD_SETSIZE; i++)
                client[i] = -1;
	tv.tv_sec = 0;
        tv.tv_usec = 0;
        while(1)
        {
                readfd = masterfd;
		writefd = masterwfd;
		int val;
                if((val = select(maxfd+1, &readfd, &writefd, NULL, &tv)) == -1)
                        perror("select: ");
		tv.tv_sec = 0; tv.tv_usec = 0;
		if(FD_ISSET(lfd, &readfd))
                {
                	if((acceptfd = accept(lfd,NULL,NULL)) == -1)
                                perror("accept: ");
                        else
                        {
				if ( (flags = fcntl (acceptfd, F_GETFL, 0)) < 0)
                			perror("F_GETFL: ");
        			flags |= O_NONBLOCK;
        			if (fcntl(lfd, F_SETFL, flags) < 0)
                			perror("F_SETFL: ");
				int flag = 1;
                                int result = setsockopt(acceptfd, IPPROTO_TCP, TCP_NODELAY,(char *) &flag,sizeof(int));
                                FD_SET(acceptfd, &masterfd);
                                if(acceptfd > maxfd)
                                        maxfd = acceptfd;
                                for(int i = 0; i < FD_SETSIZE; i++)
                                {
                                        if(client[i] == -1)
                                        {
                                                client[i] = acceptfd;
						queue[back].fd = acceptfd;
                                                queue[back].state = READ;
						queue[back].offset = 0;
						back = (back + 1)% MAXEVENTS;
						bzero(buf[i],BUF_LEN);
                                                if(i > maxind)
                                                        maxind = i;
                                                break;
                                        }
                                }
                        }
                }
                else
                {
			int efd,i;
			if(front == back)
			{
				printf("Event Queue Empty\n");
				tv.tv_sec = 2;
			}
			else
				efd = queue[front].fd;
                        for(i = 0 ; i <= maxind; i++)
                        {
				if(efd == client[i])
					break;
			}
                        if(client[i] < 0)
				continue;
			if(FD_ISSET(client[i],&readfd) && (queue[front].state == READ || queue[front].state == GETNAME || queue[front].state == STORE))
                        {
                        	//if(strlen(buf[i]) > 0)
                                //	continue;
                                bzero(buf[i],BUF_LEN);
                                if((nb = recv(client[i], buf[i], BUF_LEN, 0)) < 0)
                                {
                                        perror("recv: ");
                                        break;
                                }
                                else if(nb > 0)
                                {
                        	        //printf("%d\n",nb);
					if(queue[front].state == READ)
					{
                                        	buf[i][nb] = '\0';
                                        	printf("Reading\n");
						//printf("%s\n",buf[i]);
						queue[front].state = processcommand(buf[i]);
						//if(queue[front].state == START)
						FD_SET(client[i],&masterwfd);
						bzero(queue[front].fname,100);
						strcpy(queue[front].fname,buf[i]);
					}
					else if(queue[front].state == GETNAME)
					{	
						buf[i][nb] = '\0';
						queue[front].state = SENDFILE;
						FD_SET(client[i],&masterwfd);
					}
					else if(queue[front].state == STORE)
					{
						buf[i][nb+1] = '\0';
						_mkdir(queue[front].fname);
						FILE *fp = fopen(queue[front].fname,"ab");
						//perror("fopen");
						//printf("File Written: %s\n",queue[front].fname);
						fwrite( buf[i], 1, nb, fp);
						fclose(fp);

					}
					//send(client[i],"Start",6,0);
				        //FD_SET(client[i], &writefd);
					queue[back].fd = client[i];
					queue[back].state = queue[front].state;
					strcpy(queue[back].fname,queue[front].fname);
					back = (back + 1) % MAXEVENTS;
					front = (front + 1) % MAXEVENTS;
                                }
                                else{
                                        printf("Closed\n");
                                        close(client[i]);
                                        FD_CLR(client[i], &masterfd);
                                        //FD_CLR(client[i], &writefd);
					queue[front].offset = 0;
					front = (front + 1) % MAXEVENTS;
                                        client[i] = -1;
                                }
				/*for(int ind = front; ind < back ; ind++)
				{
					printf("Fd: %d State: %d Name:%s\n",queue[ind].fd,queue[ind].state,queue[ind].fname);
				}*/
                       	}
			else if(queue[front].state == GETNAME)
			{
				char skipchar[50];
				strcpy(skipchar,buf[i]);
				int skip = 0;
				while(skipchar[skip] && skipchar[skip] != ' ')
					skip++;
				skip++;
				queue[back].fd = client[i];
				queue[back].state = SENDFILE;
				strcpy(queue[back].fname,skipchar+skip);
                                FD_SET(client[i],&masterwfd);
				back = (back + 1) % MAXEVENTS;
				front = (front + 1) % MAXEVENTS;
			}
			else if(FD_ISSET(client[i],&readfd) && queue[front].state == RM_PROC)
			{
				struct rm_msg val;
				printf("Removing Data\n");
				if((nb = recv(client[i], &val , sizeof(val), 0)) > 0)
				{
					//printf("nb %d",nb);
					if(val.status == 0)
					{
						//printf("Dir: %s\n",val.msg);
						char remcmd[50];
						strcpy(remcmd,"rm -r ");
						if(strncmp(val.msg,"bigfs",5) == 0)
						{
							strcat(remcmd,val.msg);
							system(remcmd);
						}
					}
					else
					{
						for(int files = 0; files < val.status; files++)
						{
							char fname[100];
							char ind[4];
							sprintf(ind,"%d",files);
							strcpy(fname,val.msg);
							strcat(fname,ind);
							//printf("File: %s\n",fname);
							if( access( fname, F_OK ) != -1 ) {
								remove(fname);
							}
						}
					}
				}
				queue[back].fd = client[i];
				queue[back].state = READ;
				front = (front + 1) % MAXEVENTS;
				back = (back + 1) % MAXEVENTS;
			}
			else if(FD_ISSET(client[i],&writefd) && (queue[front].state == START || queue[front].state == RM_CMD))
			{
				send(client[i],"Start",6,0);
				FD_CLR(client[i], &masterwfd);
				queue[back].fd = client[i];
				if(queue[front].state == RM_CMD)
				{
					queue[back].state = RM_PROC;
					front = (front + 1) % MAXEVENTS;
					back = (back + 1) % MAXEVENTS;
					continue;
				}
				queue[back].state = STORE;
				//printf("Buf: %s\n",buf[i]);
				//strcpy(queue[back].fname,buf[i]);
				int skip = 0;
				while(queue[front].fname[skip] && queue[front].fname[skip] != ' ')
					skip++;
				skip++;
				strcpy(queue[back].fname,queue[front].fname+skip);
				printf("Storing File: %s\n",queue[back].fname);
				//printf("Skip %d\n",skip);
				front = (front + 1) % MAXEVENTS;
				back = (back + 1) % MAXEVENTS;
			}
			else if(FD_ISSET(client[i],&writefd) && queue[front].state == SENDFILE)
			{
				char filedata[BLOCKSIZE+1];
				bzero(filedata,BLOCKSIZE+1);
                                FILE *fp;
				int nwrites = 0;
                                //printf("SEND: %s\n",queue[front].fname);
                                fp = fopen(queue[front].fname,"rb");
				fseek(fp,queue[front].offset,SEEK_SET);
                                int nb = fread(filedata,1,BLOCKSIZE,fp);
				//queue[front].offset += nb;
				//int nb = strlen(filedata);
				//printf("Offset: %ld\n",queue[front].offset);
				fclose(fp);
				//filedata[nb] = '\0';
				//int cn = htonl(nb);
				//printf("Offset: %ld\n",queue[front].offset);
				//send(client[i],&cn,sizeof(cn),0);
				signal(SIGPIPE,SIG_IGN);
                                if((nwrites = send(client[i], filedata, nb, 0)) < 0)
                                {
					if(errno == ECONNRESET)
					{
						perror("DEB");
						FD_CLR(client[i],&masterwfd);
                                                bzero(buf[i],BUF_LEN);
                                                printf("Completed %d\n",client[i]);
                                                close(client[i]);
                                                FD_CLR(client[i], &masterfd);
                                                FD_CLR(client[i], &writefd);
						queue[front].offset = 0;
                                                front = (front + 1) % MAXEVENTS;
                                                client[i] = -1;
                                                //queue[back].fd = client[i];
                                                //queue[back].state = READ;
                                                //back = (back + 1) % MAXEVENTS;

					}
					else if(errno != EWOULDBLOCK)
                                        	perror("Write Error");
                                }
                                else
                                {
					queue[front].offset += nwrites;
                                	//TODO-Send Special Character
					if( nb < BLOCKSIZE)
					{
						FD_CLR(client[i],&masterwfd);
                                        	bzero(buf[i],BUF_LEN);
						printf("nb: %d\n",nb);
						printf("Completed\n");
                                        	//close(client[i]);
                                        	//FD_CLR(client[i], &masterfd);
                                        	FD_CLR(client[i], &writefd);
						queue[front].offset = 0;
                                        	front = (front + 1) % MAXEVENTS;
                                        	//client[i] = -1;
						queue[back].fd = client[i];
                                                queue[back].state = READ;
						queue[back].offset = 0;
						back = (back + 1) % MAXEVENTS;
						/*for(int ind = front; ind < back ; ind++)
                                		{
                                        		printf("Fd: %d State: %d Name: %s Offset: %d\n",queue[ind].fd,queue[ind].state,queue[ind].fname,queue[ind].offset);
                                		}
						for(int q = 0; q < MAXEVENTS; q++)
					                printf("q %d Fd: %d State: %d Name:%s Offset %d\n",q,queue[q].fd,queue[q].state,queue[q].fname,queue[q].offset);
						*/
                                                //front = (front + 1) % MAXEVENTS;
					}
					else
					{
						queue[back].fd = client[i];
						queue[back].state = SENDFILE;
						strcpy(queue[back].fname,queue[front].fname);
						printf("Sending File: %s\n",queue[back].fname);
						queue[back].offset = queue[front].offset;
						queue[front].offset = 0;
						back = (back + 1) % MAXEVENTS;
						front = (front + 1) % MAXEVENTS;
					}
                                }

			}
			else
			{
				queue[back].fd = client[i];
				queue[back].state = queue[front].state;
				strcpy(queue[back].fname,queue[front].fname);
				front = (front + 1) % MAXEVENTS;
				back = (back + 1) % MAXEVENTS;
			}
                }
        }
	return 0;
}
