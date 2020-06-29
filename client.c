/***************************************\ 
*       NP Assignment 2 - Part 1        *
*       Ankur Vineet (2018H1030144P)    *
*       Aman Sharma  (2018H1030137P)    *
\***************************************/

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<sys/types.h>
#include<fcntl.h>
#include<sys/stat.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<string.h>
#include<errno.h>
#include<dirent.h>
#include<netinet/tcp.h>

#define FSBLOCK 1000000
#define MAXDATANODE 25
#define MSGLENGTH 512
#define BLOCKSIZE 1000
#define METASIZE 100

#define CONNECT 4
#define SENDNAME 5
#define WRITE 6
#define FINISH 7

#define MAXCON 10
#define CP_CMD 13
#define CP_DIR 14
#define RM_CMD 15
#define CP_BIG 16
#define CAT_CMD 17

size_t CMDLENGTH = 256;
int namefd,*datafd;
char datanode[MAXDATANODE][30];
char *metadata [METASIZE];
char *blockmeta[METASIZE];
int blocknode[METASIZE];
int blockoffset[METASIZE];
struct block
{
	int position;
	int fd;
	int mode;
	int offset;
};

struct rm_msg
{
        int status;
        char msg[50];
};

struct packet
{
        int status;
	int size;
        char buffer[BLOCKSIZE];
};


int getfile(int datanode,char *src, char *dst);
int connecttoserver(char *serverip,int port,int blockmode)
{
	int sockfd;
 	struct sockaddr_in serveraddr;
        if((sockfd = socket(AF_INET,SOCK_STREAM,0))<0)
                perror("socket: ");
	int flag;
	flag = fcntl(sockfd,F_GETFL,0);
	if(blockmode == 1)
		fcntl(sockfd,F_SETFL,flag | O_NONBLOCK);
        serveraddr.sin_family = AF_INET;
        serveraddr.sin_port = htons(port);
        //inet_pton(AF_INET,argv[1],&serveraddr.sin_addr);
        serveraddr.sin_addr.s_addr = inet_addr(serverip);
        if( connect(sockfd,(struct sockaddr *) &serveraddr, sizeof(serveraddr)) < 0 && errno != EINPROGRESS)
		return -1;
	return sockfd;
}

void initializeconnection()
{
	if((namefd = connecttoserver("127.0.0.1",8080,0)) == -1)
	{
		printf("Error Connecting to Name Server\n");
		exit(1);
	}
	else{
		int flag = 1;
         	int result = setsockopt(namefd, IPPROTO_TCP, TCP_NODELAY,(char *) &flag,sizeof(int));
		printf("Connected to Name Server\n");
	}
}

int checkcommand(char *buf,char *retsrc,char *retdst)
{
	char command[CMDLENGTH];
	strcpy(command,buf);
	char *src,*dst;
	int skip = 0;
	while(command[skip] && command[skip] != ' ')
	{
		skip++;
	}
	skip++;
	src = strtok(command+skip," ");
	if(strncmp(command, "cp", 2) == 0)
	{
		struct stat fileStat;
        	if(src == NULL)
               	{
			printf("Source & Destination Path Not Specified.\n");
			return -1;
		}
		strcpy(retsrc,src);
		dst = strtok(NULL," ");
		if(dst == NULL)
		{
			printf("Destination Path Not Specified\n");
			return -1;
		}
		strcpy(retdst,dst);
		if(strncmp(dst,"bigfs",5) == 0)
		{
			if(stat(src,&fileStat) < 0)
			{
				printf("Source Path Doesn't Exist\n");
				return -1;
			}
			else if(S_ISDIR(fileStat.st_mode))
				return CP_DIR;
			else
				return CP_CMD;
		}
		else if(strncmp(src,"bigfs",5) == 0)
			return CP_BIG;
		else
		{
			printf("Specify One bigfs Path\n");
			return -1;
		}
	}
	else if(strncmp(command, "rm", 2) == 0)
	{
		if(src == NULL)
		{
			printf("Specify Path\n");
			return -1;
		}
		else if(strncmp(src,"bigfs",5) != 0)
		{
			printf("Specify bigfs Path\n");
			return -1;
		}
		else
		{
			strcpy(retsrc,src);
			return RM_CMD;
		}
	}
	else if(strncmp(command, "cat", 3) == 0)
	{
		if(src == NULL)
		{
			printf("Specify Path\n");
			return -1;
		}
		else
		{
			strcpy(retsrc,src);
			return CAT_CMD;
		}
	}
	else
		return 0;
}

void sendcommand(int serverfd, char *buf, int len)
{
	if(len == 0)
		send(serverfd,buf,strlen(buf),0);
	else
		send(serverfd,buf,len,0);
}

void printoutput(int serverfd)
{
	char msg[MSGLENGTH];
	int nb;
	while((nb = recv(serverfd, msg, MSGLENGTH, 0)) > 0)
	{
		msg[nb] = '\0';
		printf("%s\n",msg);
		if(nb < MSGLENGTH)
			break;
	}
}

int fetchdataserverlist()
{
	sendcommand(namefd,"gt dataservers",0);
	char msg[MSGLENGTH];
	int nb,nodecount = 0;
	if((nb = recv(namefd, msg, MSGLENGTH, 0)) > 0)
		msg[nb] = '\0';
	int i = 0, j = 0;
        while(i < nb)
	{
		if(msg[i] == '\n')
		{
			nodecount++;
			j = 0;
			i++;
			continue;
		}
		datanode[nodecount][j] = msg[i];
		j++;
		i++;
	}
	return nodecount;
}

void sendfile(int nodecount, char *file, char *dst)
{
	struct stat info;
	char recvbuf[CMDLENGTH];
	char sinkbuf[CMDLENGTH];
	char sendbuf[BLOCKSIZE];
	if(stat(file,&info) < 0)
	{
        	printf("Problem With File %s\n",file);
		return;
	}
	unsigned int filesize = info.st_size;
	unsigned int nblocks = (filesize/FSBLOCK) + ((filesize % FSBLOCK) != 0);
	printf("Sending File of size: %d bytes by dividing in %d blocks.\n",filesize,nblocks);
	int nlefttoconnect,nlefttowrite,nconn=0,maxfd = -1;
	fd_set readset,writeset;
	FD_ZERO(&readset);
	FD_ZERO(&writeset);
	nlefttoconnect = nblocks;
	nlefttowrite = nblocks;
	struct block blockinfo[nblocks];
	for(int i = 0 ; i < nblocks; i++)
	{
		blockinfo[i].mode = 0;
		blockinfo[i].offset = 0;
	}
	while(nlefttowrite > 0)
	{
		while(nconn < MAXCON && nlefttoconnect > 0)
		{
			for(int i = 0; i < nblocks; i++)
			{
				if(blockinfo[i].mode == 0)
				{
					int fd;
					char nodeaddr[30];
					strcpy(nodeaddr,datanode[i%nodecount]);
					char *serverip;
                			serverip = strtok(nodeaddr," ");
                			int port = atoi(strtok(NULL," "));
					blockinfo[i].mode = CONNECT;
					//printf("--->%s %d\n",serverip,port);
					errno = 0;
                			if((fd = connecttoserver(serverip,port,1)) >= 0 && errno != EINPROGRESS)
					{
						blockinfo[i].fd = fd;
						blockinfo[i].position = i;
                        			//printf("Connection Established for Block %d\n",i);
						char number[10];
						bzero(number,10);
						sprintf(number,"%d",i);
						bzero(sendbuf,MSGLENGTH);
						strcat(sendbuf,"st ");
						strcat(sendbuf,dst);
						strcat(sendbuf,number);
						sendcommand(fd,sendbuf,0);
						blockinfo[i].mode = SENDNAME;
						FD_SET(fd,&readset);
					}
                			else
					{
                        			//printf("Waiting for Dataserver %d\n",i+1);
						//perror("connect: ");
						blockinfo[i].fd = fd;
						blockinfo[i].position = i;
						FD_SET(fd,&readset);
						FD_SET(fd,&writeset);
						errno = 0;
					}
					if(fd > maxfd)
						maxfd = fd;
					break;
				}
			}
			nconn++;
			nlefttoconnect--;
		}
		
		fd_set rs,ws;
		rs = readset;
		ws = writeset;
		select(maxfd + 1, &rs, &ws, NULL, NULL);
		for(int i = 0 ; i < nblocks; i++)
		{
			int mode = blockinfo[i].mode;
			if(mode == 0 || mode == FINISH)
				continue;
			int fd = blockinfo[i].fd;
			if(mode == CONNECT && (FD_ISSET(fd,&rs) || FD_ISSET(fd,&ws)))
			{
				int error;
				socklen_t len;
				len = sizeof(error);
				if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0)
					perror("getsock ");
				if(error)
				{
					printf("Can't Connect To Server\n");
					sendcommand(namefd,"^End",0);
					return;
				}
				printf("Connection Establised for Block %d\n",blockinfo[i].position);
				FD_CLR(fd,&writeset);
                                char number[10];
                                bzero(number,10);
                                sprintf(number,"%d",i);
                                bzero(sendbuf,MSGLENGTH);
                                strcat(sendbuf,"st ");
                                strcat(sendbuf,dst);
                                strcat(sendbuf,number);
                                sendcommand(fd,sendbuf,0);
                                blockinfo[i].mode = SENDNAME;
                                FD_SET(fd,&readset);

			}
			else if(mode == SENDNAME && FD_ISSET(fd,&rs))
			{
				recv(fd,recvbuf,CMDLENGTH,0);
				if(strcmp(recvbuf,"Start") == 0)
				{
					blockinfo[i].mode = WRITE;
					FD_CLR(fd,&readset);
					FD_SET(fd,&writeset);
				}
				else
				{
					sendcommand(namefd,"^End",0);
					printf("Problem Copying File\n");
					return;
				}
			}
			else if(mode == WRITE && FD_ISSET(fd,&ws))
			{
				bzero(sendbuf,BLOCKSIZE);
				//printf("Block: %d Offset: %ld\n",i,blockinfo[i].offset);
				FILE *fp;
				fp = fopen(file,"rb");
				fseek(fp,i*FSBLOCK+blockinfo[i].offset,SEEK_SET);
				int len = fread(sendbuf,1,BLOCKSIZE,fp);
				blockinfo[i].offset += len;
				sendcommand(blockinfo[i].fd,sendbuf,len);
				fclose(fp);
				if(len < BLOCKSIZE || blockinfo[i].offset >= FSBLOCK )
				{
					nconn--;
					nlefttowrite--;
					close(blockinfo[i].fd);
					FD_CLR(fd,&writeset);
					blockinfo[i].mode = FINISH;
					char metadata[MSGLENGTH];
					sprintf(metadata,"%s%d %d %d\n",dst,i,i%nodecount,blockinfo[i].offset);
					sendcommand(namefd,metadata,0);
				}
			}
		}
	}
	sendcommand(namefd,"^End",0);
	printf("File %s Sent Successfully\n",file);
	printf("------------------------------------------\n");
	if(recv(namefd,sinkbuf,CMDLENGTH,0) < 0)
		return;
}	

int removedata(int nodes, char *src)
{
	int nb;
	struct rm_msg val;
	struct rm_msg info;
	if((nb = recv(namefd, &val , sizeof(val), 0)) > 0)
        {
		if(val.status < 0)
			return -1;
		else
		{
			char nodeaddr[30];
                	printf("%sAt Name Node.\n",val.msg);
			for(int i = 0; i < nodes; i++)
			{
                        	strcpy(nodeaddr,datanode[i]);
				int fd;
                        	char *serverip;
                        	serverip = strtok(nodeaddr," ");
                        	int port = atoi(strtok(NULL," "));
                        	if((fd = connecttoserver(serverip,port,0)) >= 0)
				{
					int flag = 1;
			                int result = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,(char *) &flag,sizeof(int));
					info.status = val.status;
					strcpy(info.msg,src);
					sendcommand(fd,"rm bigfs/",0);
					//printf("%d %s\n",info.status,info.msg);
					char recvbuf[CMDLENGTH];
					recv(fd,recvbuf,CMDLENGTH,0);
                                	if(strcmp(recvbuf,"Start") == 0)
						send(fd,&info,sizeof(info),0);
				}
				else
				{
					printf("Can't Delete At Data Node\n");
					return 0;
				}
				close(fd);
			}

		}
        }
	return 0;
}

int requestfilemeta(char *path, char *dst)
{
	int nb;
	char command[CMDLENGTH];
	char recvbuf[MSGLENGTH*4];
	bzero(recvbuf,MSGLENGTH);
	strcpy(command,"mt ");
	strcat(command,path);
	sendcommand(namefd,command,0);
	if((nb = recv(namefd,recvbuf,MSGLENGTH*4,0)) > 0)
	{
		recvbuf[nb] = '\0';
		if(strncmp(recvbuf,"No",2) == 0)
		{
			printf("%s\n",recvbuf);
			return -1;
		}
		else
		{
				//for(int i = 0 ;i < METASIZE; i++)
				//	bzero(blockmeta[i],50);
				int j = 0;
				metadata[j] = strtok(recvbuf,"\n");
				while(metadata[j] != NULL)
				{
					j++;
					metadata[j] = strtok(NULL,"\n");
				}
				for(int i = 0; i < j; i++)
				{
					blockmeta[i] = strtok(metadata[i]," ");
					blocknode[i] = atoi(strtok(NULL," "));
					blockoffset[i] = atoi(strtok(NULL," "));
					//printf("Rsk: %s %d %d\n",blockmeta[i],blocknode[i],blockoffset[i]);
				}
				return getfile(j,path,dst);
		}
	}
	return -1;
}

int getfile(int blocks, char *path, char *dstfile)
{
	char recvbuf[BLOCKSIZE+1];
	char sendbuf[MSGLENGTH];
	int nlefttoconnect,nlefttoread,nconn=0,maxfd = -1;
	fd_set readset,writeset;
	FD_ZERO(&readset);
	FD_ZERO(&writeset);
	nlefttoconnect = blocks;
	nlefttoread = blocks;
	struct block blockinfo[blocks];
	for(int i = 0 ; i < blocks; i++)
		blockinfo[i].mode = 0;
	printf("Requesting %d blocks\n",blocks);
	while(nlefttoread > 0)
	{
		while(nconn <= MAXCON && nlefttoconnect > 0)
		{
			for(int i = 0; i < blocks; i++)
			{
				if(blockinfo[i].mode == 0)
				{
					int fd;
					char nodeaddr[30];
					//printf("%s\n",datanode[blocknode[i]]);
					strcpy(nodeaddr,datanode[blocknode[i]]);
					char *serverip;
                			serverip = strtok(nodeaddr," ");
					//printf("%s %s\n",serverip,strtok(NULL," "));
                			int port = atoi(strtok(NULL," "));
					blockinfo[i].mode = CONNECT;
					//printf("--->%s %d\n",serverip,port);
					errno = 0;
                			if((fd = connecttoserver(serverip,port,1)) >= 0 && errno != EINPROGRESS)
					{	//TODO - Hanlde EINPROGRESS
						printf("err%d\n",errno);
						blockinfo[i].fd = fd;
						blockinfo[i].position = i;
                        			//printf("Connection Established for Block %d\n",i);
						bzero(sendbuf,MSGLENGTH);
						strcpy(sendbuf,"sn ");
						strcat(sendbuf,blockmeta[i]);
						//strcat(sendbuf,number);
						sendcommand(fd,sendbuf,0);
						blockinfo[i].mode = SENDNAME;
						FD_SET(fd,&readset);
					}
                			else
					{
                        			//printf("Waiting for Dataserver %d\n",i+1);
						//perror("connect: ");
						blockinfo[i].fd = fd;
						blockinfo[i].position = i;
						FD_SET(fd,&readset);
						FD_SET(fd,&writeset);
						errno = 0;
					}
					if(fd > maxfd)
						maxfd = fd;
					//printf("mfd: %d\n",maxfd);
					break;
				}
				//blockinfo[i].fd = connecttoserver();
			}
			nconn++;
			nlefttoconnect--;
		}
		//Implement Select
		fd_set rs,ws;
		rs = readset;
		ws = writeset;
		//printf("Wait Sel\n");
		select(maxfd + 1, &rs, &ws, NULL, NULL);
		//printf("Wait Sel Aft\n");
		for(int i = 0 ; i < blocks; i++)
		{
			int mode = blockinfo[i].mode;
			if(mode == 0 || mode == FINISH)
				continue;
			int fd = blockinfo[i].fd;
			//printf("fd: %d\n",fd);
			if(mode == CONNECT && (FD_ISSET(fd,&rs) || FD_ISSET(fd,&ws)))
			{
				int error;
				socklen_t len;
				len = sizeof(error);
				if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0)
					perror("getsock ");
				if(error)
				{
					printf("Can't Connect To Server\n");
					//sendcommand(namefd,"End");
					return -1;
				}
				printf("In Connection Establised for Block %d\n",blockinfo[i].position);
				FD_CLR(fd,&writeset);
                                bzero(sendbuf,MSGLENGTH);
                                strcpy(sendbuf,"sn ");
                                strcat(sendbuf,blockmeta[i]);
                                //strcat(sendbuf,number);
                                sendcommand(fd,sendbuf,0);
				//printf("Block: %s",sendbuf);
                                blockinfo[i].mode = SENDNAME;
                                FD_SET(fd,&readset);
			}
			else if(mode == SENDNAME && FD_ISSET(fd,&rs))
			{
				int nb = 0;
				struct packet dpkt;
				if((nb = recv(fd,recvbuf,BLOCKSIZE,0)) > 0)
				{
					recvbuf[nb]='\0';
					char *file;
					FILE *fp;
					file = strrchr(blockmeta[i],'/');
					//printf("%s \n",file);
					//sprintf(blockmeta[i],"%d",i);
					fp = fopen(file+1,"ab");
					fwrite(recvbuf,nb,1,fp);
					fclose(fp);
					blockinfo[i].offset += nb;
					if(blockinfo[i].offset >= blockoffset[i])
					{
						//printf("Cl Offset: %d %d %d\n",i,blockinfo[i].offset,blockoffset[i]);
						nconn--;
                                		nlefttoread--;
                                		close(blockinfo[i].fd);
						FD_CLR(blockinfo[i].fd,&writeset);
						FD_CLR(blockinfo[i].fd,&readset);
                                		blockinfo[i].mode = FINISH;
						blockinfo[i].offset = 0;
					}
				}
			}
		}
	}
	printf("File %s Received Succesfully\n",path);
	return blocks;
}

void _mkdir(const char *dir) {
        char tmp[256];
        char *p = NULL;
        size_t len;
        snprintf(tmp, sizeof(tmp),"%s",dir);
        len = strlen(tmp);
        while(tmp[len - 1] != '/' && len > 0)
                len--;
	if(len == 0)
		return;
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

void mergefiles(int blocks, char *src , char *dst, int cat)
{
	FILE *tmp = fopen("temp.tmp","wb");
	printf("Merging Files\n");
	for(int i = 0; i < blocks; i++)
	{
		char *file;
		unsigned long len,counter=0;
		file = strrchr(src,'/');
		char mergechar[100];
		char ind[4];
		sprintf(ind,"%d",i);
		strcpy(mergechar,file+1);
		strcat(mergechar,ind);
		//printf("%s\n",mergechar);
		FILE *block = fopen(mergechar,"rb");
		fseek(block,0,SEEK_END);
		len = ftell(block);
		fseek(block,0,SEEK_SET);
		char ch;
		//ch = fgetc(block);
		//while(ch != EOF)
		for(counter = 0; counter < len; counter++)
		{
			ch = fgetc(block);	
			if(cat == 0)
				fputc(ch,tmp);
			if(cat == 1)
				printf("%c",ch);
			//ch = fgetc(block);
		}
		fclose(block);
		remove(mergechar);
	}
	fclose(tmp);
	_mkdir(dst);
	if(cat == 0)
	{
		rename("temp.tmp",dst);
		printf("File %s Saved\n",dst);
	}
	else
		remove("temp.tmp");
}

void senddir(int nodes, char *src, char *dst)
{
	DIR *d;
	struct dirent *dir;
	d = opendir(src);
	int count = 0;
	char newsrc[100];
	char newdst[100];
	if(d)
	{
		while((dir = readdir(d)) != NULL)
		{
			
				strcpy(newsrc,src);
				if(newsrc[strlen(newsrc)-1] != '/')
					strcat(newsrc,"/");
				strcat(newsrc,dir->d_name);
				strcpy(newdst,dst);
				if(newdst[strlen(newdst)-1] != '/')
					strcat(newdst,"/");
				strcat(newdst,dir->d_name);	
				if(dir->d_type == DT_DIR)
				{
					//printf("Dir\n");
					if(strcmp(dir->d_name,".") != 0 && strcmp(dir->d_name,"..") != 0)
						senddir(nodes,newsrc,newdst);
				}
				else
				{
					char command[MSGLENGTH];
					strcpy(command,"cp ");
					strcat(command,newsrc);
					strcat(command," ");
					strcat(command,newdst);
					sendcommand(namefd,command,0);
					printf("Sending %s -> %s\n",newsrc,newdst);
					sendfile(nodes,newsrc,newdst);
				}
			
		}
		closedir(d);
	}
	else
		printf("Error Reading Directory\n");
}

void getdir(char *src, char *dst)
{
	char msg[MSGLENGTH];
	char newsrc[100];
	char newdst[100];
	int nb;
	char *tok;
	char command[CMDLENGTH];
	strcpy(command,"ls ");
	strcat(command,src);
	sendcommand(namefd,command,0);
	if((nb = recv(namefd,msg,MSGLENGTH,0)) > 0)
	{
		char files[50][100];
		msg[nb] ='\0';
		if(msg[nb-1] == '\n')
			msg[nb-1] = '\0';
		tok = strtok(msg,"\n");
		int ind = 0;
		while(tok != NULL)
		{
			strcpy(files[ind],tok);
			printf("%s\n",files[ind]);
			tok = strtok(NULL,"\n");
			ind++;
		}
		for(int i = 0; i < ind; i++)
		{
			if(strncmp(files[i],"D",1) == 0)
			{
				char temp[100];
				strcpy(temp,files[i]);
				char *new = temp + 4;
                                strcpy(newsrc,src);
                                if(newsrc[strlen(newsrc)-1] != '/')
                                        strcat(newsrc,"/");
                                strcpy(newdst,dst);
                                if(newdst[strlen(newdst)-1] != '/')
                                        strcat(newdst,"/");
                                strcat(newsrc,new);
                                strcat(newsrc,"/");
                                strcat(newdst,new);
                                strcat(newdst,"/");
                                getdir(newsrc,newdst);
			}
			else if(strncmp(files[i],"F",1) == 0)
                        {
				char temp[100];
                                strcpy(temp,files[i]);
                                char *new = temp + 4;
                                strcpy(newsrc,src);
                                if(newsrc[strlen(newsrc)-1] != '/')
                                        strcat(newsrc,"/");
                                strcpy(newdst,dst);
                                if(newdst[strlen(newdst)-1] != '/')
                                        strcat(newdst,"/");
                                strcat(newsrc,new);
                                strcat(newdst,new);
                                getdir(newsrc,newdst);
                                //printf("File: %s %s %s\n",new,newsrc,newdst);
                        }
			else if(strcmp(files[i],src) == 0)
                        {
                                printf("Request: src:%s ->  dst: %s\n",src,dst);
                                int blks = requestfilemeta(src,dst);
                                if(blks < 0)
                                {
                                        printf("Error Getting File Meta Data\n");
                                        //return -1;
                                }
                                mergefiles(blks,src,dst,0);
				
                        }
			
		}
		
	}
}

int main()
{
	char *buf = calloc(CMDLENGTH,sizeof(char));
	char *src = calloc(CMDLENGTH,sizeof(char));
	char *dst = calloc(CMDLENGTH,sizeof(char));
	initializeconnection();
	//sleep(10);
	int nodes = fetchdataserverlist();
	while(1)
	{
		printf("bigfs> ");
		fflush(stdout);
		int len = getline(&buf,&CMDLENGTH,stdin);
		buf[len-1] = '\0';
		//printf("%s\n",buf);
		int type = checkcommand(buf,src,dst);
		if(type < 0)
			continue;
		if(type == CP_BIG)
		{
			getdir(src,dst);
			continue;
		}
		else if(type == CAT_CMD)
		{
			int blks = requestfilemeta(src,dst);
			if(blks < 0) continue;
			if(type == CP_BIG)
				mergefiles(blks,src,dst,0);
			if(type == CAT_CMD)
				mergefiles(blks,src,dst,1);
			continue;
		}
		else if(type == CP_DIR)
		{
			senddir(nodes,src,dst);
			continue;
		}
		sendcommand(namefd,buf,0);
		if(type == CP_CMD)
			sendfile(nodes,src,dst);
			//printf("%s\n",src);
		else if(type == RM_CMD)
		{
			if(removedata(nodes,src) < 0)
				printf("Unable to Remove, Check Name\n");
		}
		else//(type != CP_CMD)
			printoutput(namefd);
	}
	close(namefd);
	//close(datafd);
	return 0;
}
