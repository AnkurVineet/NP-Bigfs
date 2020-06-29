/***************************************\ 
*       NP Assignment 2 - Part 1        *
*       Ankur Vineet (2018H1030144P)    *
*       Aman Sharma  (2018H1030137P)    *
\***************************************/
//Name Server

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<fcntl.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<sys/time.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<errno.h>

#define BLOCKSIZE 1000
#define MAXEVENTS 1000
#define BUF_LEN 512
#define DEPTH 16
#define DIR_TAB "directory_table.dat"
#define READ 5
#define PROCESS 6
#define STOREMETA 7
#define GT_CMD 10
#define LS_CMD 11
#define MV_CMD 12
#define CP_CMD 13
#define RM_CMD 15
#define MT_CMD 16
#define GETNAME 17
#define SENDFILE 18
#define STOREDONE 19

struct event
{
	int fd;
	int state;
	char fname[100];
};
struct rm_msg
{
	int status;
	char msg[50];
};

char path[BUF_LEN];
int maxfileid = 0;

struct record
{
	int id;
	int pid;
	char name[50];
	char type;
	char metafile[100];
};

void preparefilehierarchy()
{
	FILE *fp;
	if(access(DIR_TAB, F_OK) == -1)
	{
		fp = fopen(DIR_TAB, "w");
		struct record entry1 = {1, 0, "bigfs", 'D',""};
		struct record entry2 = {2, 1, "new", 'D', ""};
		struct record entry3 = {3, 2, "test", 'F',"bigfs_new_test_.meta"};
		struct record entry4 = {4, 2, "test2", 'F',"bigfs_new_test2_.meta"};
		fwrite( &entry1, sizeof(struct record), 1, fp);
		fwrite( &entry2, sizeof(struct record), 1, fp);
		fwrite( &entry3, sizeof(struct record), 1, fp);
		fwrite( &entry4, sizeof(struct record), 1, fp);
		fclose(fp);
		maxfileid = 4;
	}
	else
	{
		fp = fopen(DIR_TAB, "r");
		struct record entry;
		while(fread(&entry, sizeof(struct record), 1, fp))
		{
			if(entry.id > maxfileid)
				maxfileid = entry.id;
		}
		fclose (fp);
	}
}

void removeentry(struct record entry)
{
	FILE *meta = fopen(DIR_TAB,"r");
	FILE *tmp = fopen("temp.dat","w");
	struct record copy;
	while(fread(&copy,sizeof(copy),1,meta))
	{
		if(copy.id == entry.id)
		{
			remove(entry.metafile);
			continue;
		}
		else
			fwrite(&copy,sizeof(copy),1,tmp);
	}
	fclose(meta);
	fclose(tmp);
	remove(DIR_TAB);
	rename("temp.dat",DIR_TAB);
}
struct record getentry(char *pathval)
{
	char *dirs[DEPTH];
	char path[BUF_LEN];
	strcpy(path,pathval);
	int i = 0;
	struct record entry;
	entry.id = 0;
	dirs[i] = strtok(path,"/");
	while( dirs[i] != NULL )
	{
      		//printf("%s\n", dirs[i]);
		i++;
      		dirs[i] = strtok(NULL, "/");
	}
	int id = 0;
	int typefile = 0;
	i = 0;
	while(dirs[i] != NULL)
	{
		int flag = 1;
		typefile = 0;
		FILE *fp;
		fp = fopen (DIR_TAB, "r");
		if (fp == NULL)
		{
			fprintf(stderr, "\nError opening file structure\n");
    		}
		while(fread(&entry, sizeof(struct record), 1, fp))
		{
			printf ("id = %d,pid = %d, name = %s type= %c, mfile = %s \n", entry.id,entry.pid,entry.name,entry.type,entry.metafile);
			if(entry.pid == id && strcmp(entry.name,dirs[i]) == 0)
			{
				id = entry.id;
				i++;
				flag = 0;
				if(entry.type == 'F')
					typefile = 1;
				break;
			}
		}
		fclose(fp);
		if(flag == 1)
		{
			entry.id = -2;
			return entry;
		}
	}
	//if(typefile == 1 && type == LS_CMD)
	//	return -3;
	return entry;
}

int removecommand(char *command)
{
	int skip = 0;
	while(command[skip] && command[skip] != ' ')
	{
		//printf("%c\n",command[skip]);
		skip++;
	}
	skip++;
	return skip;
}

int processcommand(char *command)
{
	if(strncmp(command, "gt", 2) == 0)
		return GT_CMD;
	if(strncmp(command, "ls", 2) == 0)
		return LS_CMD;
	if(strncmp(command, "mv", 2) == 0)
		return MV_CMD;
	if(strncmp(command, "cp", 2) == 0)
		return CP_CMD;
	if(strncmp(command, "rm", 2) == 0)
		return RM_CMD;
	if(strncmp(command, "mt", 2) == 0)
		return MT_CMD;
	return -1;
}

char * senddsmeta()
{
	FILE *fp;
	fp = fopen("datanodes.txt","r");
	char *line,*msg;
	msg = (char *) calloc(BUF_LEN, sizeof(char));
	int read;
	ssize_t len = 0;
	while ((read = getline(&line, &len, fp)) != -1)
	{
        	//printf("Retrieved line of length %d:\n", read);
        	//printf("%s", line);
		strcat(msg,line);
    	}
	fclose(fp);
	return msg;
}

char * processlist(char *command)
{
	char *msg;
	int skip = removecommand(command);
	struct record entry;
	msg = (char *) calloc(BUF_LEN, sizeof(char));
	strcpy(path,command+skip);
	struct record lsentry = getentry(command+skip);
	if(lsentry.id == -2)
	{
		strcat(msg,"No Such File or Directory");
		return msg;
	}
	if(lsentry.type == 'F')
		return path;
	FILE *fp;
	fp = fopen (DIR_TAB, "r");
	if (fp == NULL)
	{
		fprintf(stderr, "\nError opening file structure\n");
    	}
	while(fread(&entry, sizeof(struct record), 1, fp))
	{
		if(entry.pid == lsentry.id)
		{
			char type[2];
			sprintf(type,"%c",entry.type);
			strcat(msg,type);
			strcat(msg," : ");
			strcat(msg,entry.name);
			strcat(msg,"\n");
		}
	}
	fclose(fp);
	return msg;
}

char* moverecord(char *command)
{
	int skip = removecommand(command);
	//printf("%s\n",command+skip);
	printf("Moving Record\n");
	char *src,*dst,*msg;
	struct record srcentry, dstentry, entry;
	msg = (char *) calloc(BUF_LEN, sizeof(char));
	src = strtok(command+skip," ");
	if(src == NULL)
		return "Invalid Move Operation";
	dst = strtok(NULL," ");
	if(dst == NULL)
		return "Invalid Move Operation";
	if(strncmp(src, dst, strlen(src)) == 0)
		strcpy(msg,"Invalid Move Operation");
	else
	{
		srcentry = getentry(src);
		dstentry = getentry(dst);
		printf("%d %d %s %c\n",srcentry.id,srcentry.pid,srcentry.name,srcentry.type);
		printf("%d %d %s %c\n",dstentry.id,dstentry.pid,dstentry.name,dstentry.type);
		if(dstentry.id == -2)
		{
			strcpy(msg,"Destination File Not Present");
		}
		else if(dstentry.type == 'D')
		{
			int records=0;
			FILE *fp = fopen(DIR_TAB,"rb+");
			while(fread(&entry,sizeof(entry),1,fp)==1)
			{
				if(entry.id == srcentry.id)
				{
					entry.pid = dstentry.id;
					fseek(fp,sizeof(struct record)*records,SEEK_SET);
					fwrite(&entry,sizeof(entry),1,fp);
				}
				records++;
			}
			fclose(fp);
			strcpy(msg,"Succefully Moved.");
		}
		else if(srcentry.type == 'F' && dstentry.type == 'F')
		{
			rename(srcentry.metafile,dstentry.metafile);
			removeentry(srcentry);
			strcpy(msg,"Succefully Moved.");
		}
		else
			strcpy(msg,"Invalid Move Operation.");
	}
	return msg;
}

char* startcopy(char* command)
{
	int skip = removecommand(command),i = 0,j = 0, pid = 0;
	char *existingpath;
	existingpath = (char *) calloc(BUF_LEN, sizeof(char));
	char *dst = strtok(command+skip," ");
	char *msg;
	msg = (char *) calloc(BUF_LEN, sizeof(char));
	struct record entry;
	dst = strtok(NULL," ");
	strcpy(msg,dst);
	char *dirs[DEPTH];
        dirs[i] = strtok(dst,"/");
	bzero(existingpath,BUF_LEN);
        while( dirs[i] != NULL )
        {
                i++;
                dirs[i] = strtok(NULL, "/");
        }
	for(j = 0; j < i; j++)
	{
		strcat(existingpath,dirs[j]);
		entry = getentry(existingpath);
		strcat(existingpath,"/");
		//printf("exist:%s.\n",existingpath);
		if(entry.id == -2)
			break;
		else
			pid = entry.id;
	}
	for(int ind = 0 ; ind <=strlen(msg); ind++)
	{
		if(msg[ind] == '/')
			msg[ind] = '_';
	}
	strcat(msg,".meta");
	FILE *fp;
	fp = fopen(DIR_TAB,"a");
	while(j < i)
	{
		entry.id = ++maxfileid;
		entry.pid = pid;
		pid = entry.id;
		strcpy(entry.name,dirs[j]);
		if(j == i-1)
		{
			entry.type = 'F';
			strcpy(entry.metafile,msg);
			FILE *fmeta;
			//printf("Fname %s\n",entry.metafile);
			fmeta = fopen(entry.metafile,"w");
			fclose(fmeta);
		}
		else
			entry.type = 'D';
		fwrite(&entry, sizeof(struct record), 1, fp);
		j++;
	}
	fclose(fp);
	return msg;
}

void removedirectory(struct record entry)
{
	struct record check;
	FILE *metacopy = fopen("copy.dat","r");
	while(fread(&check,sizeof(check),1,metacopy))
	{
		if(check.type == 'D' && entry.id == check.pid)
			removedirectory(check);
		if(entry.id == check.pid)
			removeentry(check);
	}
	removeentry(entry);
	fclose(metacopy);
	//remove("copy.dat");
}

struct rm_msg removemeta(char *path)
{
	int skip = removecommand(path);
	struct record entry;
	struct rm_msg val;
	entry = getentry(path + skip);
	if(entry.id == -2)
	{
		val.status = -2;
		strcpy(val.msg,"No Such File or Directory\n");
		return val;
	}
	else if(entry.type == 'D')
	{
		val.status = 0;
		char copy[100];
		strcpy(copy,"cp ");
		strcat(copy,DIR_TAB);
		strcat(copy," copy.dat");
		system(copy);
		removedirectory(entry);
		strcpy(val.msg,"Directory Removed Successfully\n");
		remove("copy.dat");
		return val;
	}
	else
	{
		FILE *fptr = fopen(entry.metafile,"r");
		char ch;
		int count = 0;
		ch = fgetc(fptr);
		while(ch != EOF)
		{
			if(ch == '\n')
				count++;
			ch = fgetc(fptr);
		}
		fclose(fptr);
		val.status = count;
		//printf("Count:%d\n",count);
		removeentry(entry);
		strcpy(val.msg,"File Removed Successfully\n");
	}
	return val;
}

char * filemeta(char *path)
{
	int skip = removecommand(path);
	struct record entry = getentry(path + skip);
	char *msg;
	msg = (char *) calloc(BUF_LEN*4, sizeof(char));
	if(entry.id == -2)
	{
		strcpy(msg,"No Such File or Directory");
		return msg;
	}
	else
	{
		if(entry.type == 'D')
		{
			strcpy(msg,"No Meta Record Found");
                	return msg;
		}
		FILE *meta = fopen(entry.metafile,"r");
		fread(msg,BUF_LEN*4,1,meta);
		fclose(meta);
		return msg;
	}

}
int main()
{
	fd_set masterfd,masterwfd,readfd,writefd;
	struct record entry;
	int lfd,connfd,flags,maxfd,acceptfd,nb,maxind,front = 0,back = 0;
	char buf[FD_SETSIZE][BUF_LEN];
	int client[FD_SETSIZE];
	struct sockaddr_in serveraddr;
	struct event queue[MAXEVENTS];
	struct timeval tv;
	preparefilehierarchy();
        lfd = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&serveraddr, sizeof(serveraddr));
        serveraddr.sin_family = AF_INET;
        serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
        serveraddr.sin_port = htons(8080);
        if(bind(lfd,(struct sockaddr *)&serveraddr,sizeof(serveraddr)) == -1)
                perror("bind: ");
        listen(lfd,10);
	if ( (flags = fcntl (lfd, F_GETFL, 0)) < 0)
 		perror("F_GETFL: ");
	flags |= O_NONBLOCK;
	if (fcntl(lfd, F_SETFL, flags) < 0)
 		perror("F_SETFL: ");
	FD_ZERO(&masterfd);
	FD_ZERO(&masterwfd);
	FD_ZERO(&readfd);
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
		if(select(maxfd+1, &readfd, &writefd, NULL, &tv) == -1)
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
			/*for(int ind = front; ind < back ; ind++)
                        {
	                        printf("Fd: %d State: %d Name:%s\n",queue[ind].fd,queue[ind].state,queue[ind].fname);
                        }*/
			if(client[i] < 0)
				continue;
			if(FD_ISSET(client[i],&readfd) && (queue[front].state == READ || queue[front].state == STOREMETA || queue[front].state == GETNAME))
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
					buf[i][nb] = '\0';
					printf("Reading\n");
					/*for(int ind = front; ind < back ; ind++)
                                        {
                                        	printf("Fd: %d State: %d Name:%s\n",queue[ind].fd,queue[ind].state,queue[ind].fname);
                                        }*/
					//printf("State: %d\n",queue[front].state);
					if(queue[front].state == READ)
						queue[front].state = PROCESS;
					if(queue[front].state == STOREMETA)
					{
						//printf("meta: %s\n",buf[i]);
						char *ch;
						int flag = 0;
						if((ch = strrchr(buf[i],'^')) != NULL)
						{	ch[0] = '\0'; flag = 1;}
						FILE *fp = fopen(queue[front].fname,"a");
						fwrite(buf[i], strlen(buf[i]), 1 , fp);
						fclose(fp);
						if(flag == 1)
						{
							queue[back].fd = client[i];
							queue[back].state = STOREDONE;
							FD_SET(client[i],&masterwfd);
							back = (back + 1) % MAXEVENTS;
							front = (front + 1) % MAXEVENTS;
							continue;
						}
						//strcpy(buf[i],startcopy(buf[i]));
						//printf("%s\n",buf[i]);
					}
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
					front = (front + 1) % MAXEVENTS;
					FD_CLR(client[i], &masterfd);
					FD_CLR(client[i], &masterwfd);
					client[i] = -1;
				}
			}
			else if(queue[front].state == PROCESS)
			{
				printf("Processing\n");
				queue[back].fd = client[i];
				queue[back].state = processcommand(buf[i]);
				strcpy(queue[back].fname,queue[front].fname);
				if(queue[back].state != CP_CMD)
					FD_SET(client[i],&masterwfd);
				back = (back + 1) % MAXEVENTS;
                                front = (front + 1) % MAXEVENTS;
			}
			else if(queue[front].state == CP_CMD)
			{
				queue[back].fd = client[i];
				queue[back].state = STOREMETA;
				strcpy(queue[back].fname,startcopy(buf[i]));
                                //printf("%s\n",queue[back].fname);
				back = (back + 1) % MAXEVENTS;
                                front = (front + 1) % MAXEVENTS;
			}
			else if(FD_ISSET(client[i], &writefd) && (queue[front].state == GT_CMD || queue[front].state == LS_CMD || queue[front].state == MV_CMD || queue[front].state == RM_CMD || queue[front].state == MT_CMD))
			{
				switch(processcommand(buf[i]))
				{
					char sendbuf[BUF_LEN*4];
					struct rm_msg val;
					case GT_CMD:
						strcpy(sendbuf,senddsmeta());
						if(send(client[i], sendbuf, strlen(sendbuf), 0) < 0)
                                                {
                        	                        if(errno != EWOULDBLOCK)
                                	                        perror("Write Error");
                                        	}
                                                else
						{
                                                        bzero(buf[i],BUF_LEN);
							queue[back].fd = client[i];
							queue[back].state = READ;
							back = (back + 1) % MAXEVENTS;
                			                front = (front + 1) % MAXEVENTS;
                                                }
                                                break;
					case LS_CMD:
						strcpy(sendbuf,processlist(buf[i]));
						if(send(client[i], sendbuf, strlen(sendbuf), 0) < 0)
						{
							if(errno != EWOULDBLOCK)
								perror("Write Error");
						}
						else
						{
							bzero(buf[i],BUF_LEN);
							queue[back].fd = client[i];
                                                        queue[back].state = READ;
                                                        back = (back + 1) % MAXEVENTS;
                                                        front = (front + 1) % MAXEVENTS;
						}
						break;
					case MV_CMD:
						strcpy(sendbuf,moverecord(buf[i]));
						if(send(client[i], sendbuf, strlen(sendbuf), 0) < 0)
						{
							if(errno != EWOULDBLOCK)
								perror("Write Error");
						}
						else
						{
							bzero(buf[i],BUF_LEN);
							queue[back].fd = client[i];
                                                        queue[back].state = READ;
                                                        back = (back + 1) % MAXEVENTS;
							front = (front + 1) % MAXEVENTS;
						}
						break;
					case RM_CMD:
						//strcpy(sendbuf,removemeta(buf[i]));
						val = removemeta(buf[i]);
						if(send(client[i], &val, sizeof(val), 0) < 0)
                                                {
                                                        if(errno != EWOULDBLOCK)
                                                                perror("Write Error");
                                                }
                                                else
                                                {
                                                        bzero(buf[i],BUF_LEN);
                                                        queue[back].fd = client[i];
                                                        queue[back].state = READ;
                                                        back = (back + 1) % MAXEVENTS;
                                                        front = (front + 1) % MAXEVENTS;
                                                }
						break;
					case MT_CMD:
						strcpy(sendbuf,filemeta(buf[i]));
						if(send(client[i], sendbuf, strlen(sendbuf), 0) < 0)
						{
							if(errno != EWOULDBLOCK)
								perror("Write Error");
						}
						else
						{
							bzero(buf[i],BUF_LEN);
							queue[back].fd = client[i];
                                                        queue[back].state = READ;
                                                        back = (back + 1) % MAXEVENTS;
							front = (front + 1) % MAXEVENTS;
						}
						break;
					case STOREDONE:;
						printf("Storing Done\n");
						if(send(client[i], "Done", 4, 0) < 0)
                                                {
                                                        if(errno != EWOULDBLOCK)
                                                                perror("Write Error");
                                                }
                                                else
                                                {
                                                        bzero(buf[i],BUF_LEN);
                                                        queue[back].fd = client[i];
                                                        queue[back].state = READ;
                                                        back = (back + 1) % MAXEVENTS;
                                                        front = (front + 1) % MAXEVENTS;
                                                }
						break;
					default:
						bzero(buf[i],BUF_LEN);
				}
				FD_CLR(client[i],&masterwfd);
			}
			else if(FD_ISSET(client[i], &writefd) && queue[front].state == STOREDONE)
			{
				printf("Storing Done\n");
				if(send(client[i], "Done", 4, 0) < 0)
                                {
        	                        if(errno != EWOULDBLOCK)
                	                        perror("Write Error");
                        	}
                                else
                                {
                                        bzero(buf[i],BUF_LEN);
                                        queue[back].fd = client[i];
                                        queue[back].state = READ;
                                        back = (back + 1) % MAXEVENTS;
                                        front = (front + 1) % MAXEVENTS;
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
