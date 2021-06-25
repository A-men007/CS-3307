#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define TRUE 1
#define FALSE 0

/* Create a structure that would represent an account */
typedef struct _Account
{
	char id[5];
	char type[10];
	int depositFee;
	int withdrawalFee;
	int transferFee;
	int transactionFee;
	int transactionFeeThreshold;
	int isOverdraftProtected;
	int overdraftFee;
	int balance;
	int numTransactions;
	
	/* Pointer to a next account (it's a linked list) */
	struct _Account *next;
	
	/* Each account will be protected by a mutex */
	pthread_mutex_t lock;
} Account;

/* Holds the list of accounts */
typedef struct _AccountsList
{
	Account *head;
	Account *tail;
	int numAccounts;
} AccountsList;

/* A job represents a deposit, withdraw, or fundtransfer */
typedef struct _Job
{
	char type;
	Account *fromAccount;
	Account *toAccount;
	int amount;
		
	/* POinter to the next job */
	struct _Job *next;	
} Job;

/* Create a structure that holds a thread */
typedef struct _Transaction
{
	pthread_t thread;
	char id[10];
	Job *jobsHead;
	Job *jobsTail;
	
	/* Pointer to the next transaction (it's a linked list */
	struct _Transaction *next;
} Transaction;

/* Holds the list of transactions */
typedef struct _TransactionsList
{
	Transaction *transactions;
	int numTransactions;
} TransactionsList;

/* Global variables */
AccountsList accountsList;
TransactionsList transactionsList;
int numDepositorsRunning = 0;
int numDepositorsFinished = 0;

/* A very special lock when performing fund transfer to avoid deadlock 
Deadlock happens when A1 wants to transfer to A2 and A2 wants to transfer to A1
They'll be waiting each other's lock forever so we use this lock to make sure
that only 1 account at a time can do transfers */
pthread_mutex_t transferFundsLock = PTHREAD_MUTEX_INITIALIZER; 

/* Another special lock that blocks client from executing until all depositors are done */
pthread_mutex_t clientsLock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t clientsWait = PTHREAD_COND_INITIALIZER;

/* Create an account object of the details and adds it to the accounts list */
void addAccount(char *line)
{
	Account *account;
	char *token;
	
	account = (Account *) malloc(sizeof(Account));
	account->next = NULL;
	account->balance = 0;
	account->numTransactions = 0;
	
	pthread_mutex_init(&account->lock, NULL);
	
	/* First token will always be the ID */
	token = strtok(line, " ");
	strcpy(account->id, token);
	
	token = strtok(NULL, " ");
	while(token != NULL)
	{
		if(strcmp(token, "type") == 0)
		{
			/* Extract the account type */
			token = strtok(NULL, " ");
			strcpy(account->type, token);
		}
		else if(strcmp(token, "d") == 0)
		{
			/* Extract the deposit fee */
			token = strtok(NULL, " ");
			sscanf(token, "%d", &account->depositFee);
		}
		else if(strcmp(token, "w") == 0)
		{
			/* Extract the withdrawal fee */
			token = strtok(NULL, " ");
			sscanf(token, "%d", &account->withdrawalFee);
		}
		else if(strcmp(token, "t") == 0)
		{
			/* Extract the transfer fee */
			token = strtok(NULL, " ");
			sscanf(token, "%d", &account->transferFee);
		}
		else if(strcmp(token, "transactions") == 0)
		{
			/* Extract the transaction fee limit before fee can occur */
			token = strtok(NULL, " ");
			sscanf(token, "%d", &account->transactionFeeThreshold);
			
			token = strtok(NULL, " ");
			sscanf(token, "%d", &account->transactionFee);
		}
		else if(strcmp(token, "overdraft") == 0)
		{
			/* Extract and check if account is overdraft protected */
			token = strtok(NULL, " ");
			
			if(strcmp(token, "Y") == 0)
			{
				account->isOverdraftProtected = TRUE;
				
				token = strtok(NULL, " ");
				sscanf(token, "%d", &account->overdraftFee);
			}
			else
			{
				account->isOverdraftProtected = FALSE;
			}
		}
		
		token = strtok(NULL, " ");
	}
	
	/* Add account to the list */
	if(accountsList.numAccounts == 0)
	{
		accountsList.head = account;
		accountsList.tail = account;
	}
	else
	{
		accountsList.tail->next = account;
		accountsList.tail = account;
	}
	
	accountsList.numAccounts++;
}

/* Find the account object that holds the ID */
Account *findAccount(char *id)
{
	Account *current;
	
	current = accountsList.head;
	
	while(current != NULL)
	{
		if(strcmp(current->id, id) == 0)
			return current;
			
		current = current->next;
	}
	
	return NULL;
}

/* Delete all accounts */
void deleteAccounts()
{
	Account *next;
	Account *current;
	
	next = NULL;
	current = accountsList.head;
	
	while(current != NULL)
	{
		next = current->next;
		free(current);
		current = next;
	}
}

/* Delete all jobs */
void deleteJobs(Job *jobs)
{
	Job *next;
	Job *current;
	
	next = NULL;
	current = jobs;
	
	while(current != NULL)
	{
		next = current->next;
		free(current);
		current = next;
	}
}

/* Delete all transactions */
void deleteTransactions()
{
	Transaction *next;
	Transaction *current;
	
	next = NULL;
	current = transactionsList.transactions;
	
	while(current != NULL)
	{
		next = current->next;
		deleteJobs(current->jobsHead);
		free(current);
		current = next;
	}
}

/* Print all the accounts */
void printAccounts(FILE *outFile)
{
	Account *current;
	
	current = accountsList.head;
	
	while(current != NULL)
	{
		fprintf(outFile, "%s type %s %d\n", 
			current->id,
			current->type,
			current->balance);
		current = current->next;
	}
}

/* Deposit an amount to an account, fees only apply for clients and not for depositors */
void depositToAccount(Account *account, int amount, int applyFee)
{
	int fees;
	
	pthread_mutex_lock(&account->lock);

	printf("Depositing $%d to account %s with starting balance of $%d\n", amount, account->id, account->balance);
			
	/* Calculate any added fees */
	fees = 0;	
	
	if(applyFee)
	{
		fees = account->depositFee;
		printf("    Deposit fee of $%d\n", account->depositFee);
	}
	
	if(applyFee && account->numTransactions > account->transactionFeeThreshold)
	{
		printf("    Transaction fee of $%d, (made %d transactions out of %d transactions limit)\n", 
			account->transactionFee, account->numTransactions, account->transactionFeeThreshold);
		fees += account->transactionFee;
	}
	
	account->balance += amount;
	
	if(applyFee)
		account->balance -= fees;
	
	account->numTransactions++;	
	printf("    Ending balance of $%d\n", account->balance);
	printf("\n");

	pthread_mutex_unlock(&account->lock);
}

/* Withdraw from account */
void withdrawFromAccount(Account *account, int amount)
{
	int fees;
	int num500s;

	pthread_mutex_lock(&account->lock);

	printf("Withdrawing $%d from account %s with starting balance of $%d\n", amount, account->id, account->balance);

	/* Calculate any added fees */			
	fees = account->withdrawalFee;
	printf("    Withdrawal fee of $%d\n", account->withdrawalFee);
	
	if(account->numTransactions > account->transactionFeeThreshold)
	{
		printf("    Transaction fee of $%d, (made %d transactions out of %d transactions limit)\n", 
			account->transactionFee, account->numTransactions, account->transactionFeeThreshold);
		fees += account->transactionFee;	
	}
	
	/* Check balance... */
	if(account->balance >= amount + fees)
	{				
		/* Safe side... there's enough balance to withdraw */
		account->balance -= amount;
		account->balance -= fees;
		account->numTransactions++;				
	}
	else if(account->isOverdraftProtected)
	{
		/* Negative balance side.. applicable only for overdraft protected accounts */
		num500s = (amount / 500) + 1;
		fees += num500s * account->overdraftFee;
		
		printf("    Overdraft fee of $%d ($%d fee for every excess of $500)\n", num500s * account->overdraftFee, account->overdraftFee);
	
		/* Debt shouldn't go below -5000 */				
		if(account->balance - fees - amount >= -5000)
		{
			account->balance -= amount;
			account->balance -= fees;
			account->numTransactions++;
		}
		else
		{
			printf("    Withdrawal rejected, amount (with fees) cannot continue \n");
			printf("        because overdraft limit cannot go above $5000\n");
		}
	}
	else
	{
		printf("    Withdrawal rejected, amount (with fees) cannot continue \n");
		printf("        because of insufficient balance and account not overdraft protected\n");
	}
	
	printf("    Ending balance of $%d\n", account->balance);	
	printf("\n");

	pthread_mutex_unlock(&account->lock);
}

/* Transfer a fund from one account to another */
void transferFundsFromAndToAccount(Account *fromAccount, Account *toAccount, int amount)
{
	int senderFees;
	int receiverFees;
	int num500s;
	
	pthread_mutex_lock(&transferFundsLock);
	pthread_mutex_lock(&fromAccount->lock);	
	pthread_mutex_lock(&toAccount->lock);
	
	printf("Transferring $%d from %s to %s\n", amount, fromAccount->id, toAccount->id);
	printf("    Account (Sender) %s has starting balance of $%d\n", fromAccount->id, fromAccount->balance);
	printf("    Account (Receiver) %s has starting balance of $%d\n", toAccount->id, toAccount->balance);
	
	senderFees = fromAccount->transferFee;
	receiverFees = toAccount->transferFee;
	
	printf("    Account (Sender) %s has transfer fee of $%d\n", fromAccount->id, fromAccount->transferFee);
	printf("    Account (Receiver) %s has transfer fee of $%d\n", toAccount->id, toAccount->transferFee);
	
	if(fromAccount->numTransactions > fromAccount->transactionFeeThreshold)
	{
		printf("    Account (Sender) %s has transaction fee of $%d, (made %d transactions out of %d transactions limit)\n", 
			fromAccount->id, fromAccount->transactionFee, 
			fromAccount->numTransactions, fromAccount->transactionFeeThreshold);
		senderFees += fromAccount->transactionFee;
	}
	
	if(toAccount->numTransactions > toAccount->transactionFeeThreshold)
	{
		printf("    Account (Receiver) %s has transaction fee of $%d, (made %d transactions out of %d transactions limit)\n", 
			toAccount->id, toAccount->transactionFee, 
			toAccount->numTransactions, toAccount->transactionFeeThreshold);
		receiverFees += toAccount->transactionFee;
	}

	/* Overdraft is not applicable for fund transfer, we assume that the account where to get money
	has enough balance to transfer */		
	if(fromAccount->balance >= amount + senderFees)
	{
		/* Safe side no penalties */
		fromAccount->balance -= amount;
		fromAccount->balance -= senderFees;
		fromAccount->numTransactions++;	
		
		toAccount->balance += amount;
		toAccount->balance -= receiverFees;
		toAccount->numTransactions++;
	}
	else if(fromAccount->isOverdraftProtected)
	{
		/* Negative balance side.. applicable only for overdraft protected accounts */
		num500s = (amount / 500) + 1;
		senderFees += num500s * fromAccount->overdraftFee;
		
		printf("    Account %s (Sender) has overdraft fee of $%d ($%d fee for every excess of $500)\n", 
			fromAccount->id,
			num500s * fromAccount->overdraftFee, fromAccount->overdraftFee);
	
		/* Debt shouldn't go below -5000 */				
		if(fromAccount->balance - senderFees - amount >= -5000)
		{
			fromAccount->balance -= amount;
			fromAccount->balance -= senderFees;
			fromAccount->numTransactions++;
		}
		else
		{
			printf("    Transfer rejected, amount (with fees) cannot continue \n");
			printf("        because overdraft limit cannot go above $5000 for sender\n");
		}
	}
	else
	{
		printf("    Transfer rejected, amount (with fees) cannot continue \n");
		printf("        because of insufficient balance of sender\n");
	}
	
	printf("    Account %s has ending balance of $%d\n", fromAccount->id, fromAccount->balance);	
	printf("    Account %s has ending balance of $%d\n", toAccount->id, toAccount->balance);
	printf("\n");

	pthread_mutex_unlock(&toAccount->lock);
	pthread_mutex_unlock(&fromAccount->lock);
	pthread_mutex_unlock(&transferFundsLock);
}

/* A method for each transaction thread, runs in parallel */
void *transactionThread(void *args)
{
	Transaction *transaction;
	Job *currentJob;
	
	/* Extract transaction information */
	transaction = (Transaction *) args;
	
	if(transaction->id[0] == 'c')
	{	
		/* Clients have to wait for all depositors to finish before continuing */
		pthread_mutex_lock(&clientsLock);
		
		while(numDepositorsFinished < numDepositorsRunning)
			pthread_cond_wait(&clientsWait, &clientsLock);		
			
		pthread_cond_signal(&clientsWait);
		pthread_mutex_unlock(&clientsLock);	
	}
	
	printf("%s thread is running...\n", transaction->id);
	
	/* Do all sequence of transaction */
	currentJob = transaction->jobsHead;
	
	while(currentJob != NULL)
	{
		if(currentJob->type == 'd')
		{
			/* Perform a deposit on an account */			
			printf("%s deposit $%d to account %s\n", transaction->id, currentJob->amount, currentJob->fromAccount->id);
			
			/* Fees apply only to clients and not to depositors */
			if(transaction->id[0] == 'd')
				depositToAccount(currentJob->fromAccount, currentJob->amount, FALSE);
			else
				depositToAccount(currentJob->fromAccount, currentJob->amount, TRUE);
		}
		else if(currentJob->type == 'w')
		{
			/* Peform a withdrawal on an account */			
			printf("%s withdraw $%d from account %s\n", transaction->id, currentJob->amount, currentJob->fromAccount->id);
			
			withdrawFromAccount(currentJob->fromAccount, currentJob->amount);
		}
		else if(currentJob->type == 't')
		{
			/* Perform a fund transferfrom one account to another */			
			printf("%s transfers $%d from account %s to account %s\n", transaction->id, 
				currentJob->amount, currentJob->fromAccount->id, currentJob->toAccount->id);
			
			transferFundsFromAndToAccount(currentJob->fromAccount, currentJob->toAccount, currentJob->amount);
		}
		
		currentJob = currentJob->next;		
	}
	
	/* A depositor updates when it is done and signals clients if its ready for them to go */
	if(transaction->id[0] == 'd')
	{
		numDepositorsFinished++;
		pthread_cond_signal(&clientsWait);
		pthread_mutex_unlock(&clientsLock);
	}
	
	printf("%s finished...\n", transaction->id);
	
	return (void *) NULL;
}

/* Create a transaction and add it to the list, each trasaction will have a list of job */
Transaction *addTransaction(char *line)
{
	Transaction *transaction;
	char *token;
	Job *job;
	
	transaction = (Transaction *)malloc(sizeof(Transaction));
	transaction->id[0] = '\0';
	transaction->jobsHead = NULL;
	transaction->jobsTail = NULL;
	transaction->next= NULL;
		
	/* Extract the ID */
	token = strtok(line, " ");
	strcpy(transaction->id, token);
	
	/* Extract the jobs */
	token = strtok(NULL, " ");
	
	while(token != NULL)
	{
		job = (Job *)malloc(sizeof(Job));
		job->type = token[0];
		job->fromAccount = NULL;
		job->toAccount = NULL;
		job->next = NULL;
		
		if(job->type == 'd' || job->type == 'w')
		{
			/* Create a deposit job */
			token = strtok(NULL, " ");
			job->fromAccount = findAccount(token);
			
			token = strtok(NULL, " ");
			sscanf(token, "%d", &job->amount);
		}
		else if(job->type == 't')
		{
			/* Create a withdraw job */
			token = strtok(NULL, " ");
			job->fromAccount = findAccount(token);
			
			token = strtok(NULL, " ");
			job->toAccount = findAccount(token);
			
			token = strtok(NULL, " ");
			sscanf(token, "%d", &job->amount);
		}
		
		if(transaction->jobsHead == NULL)
		{
			transaction->jobsHead = job;
			transaction->jobsTail = job;
		}
		else
		{
			transaction->jobsTail->next = job;
			transaction->jobsTail = job;
		}
		
		token = strtok(NULL, " ");
	}
	
	/* Add the job to the list */
	transaction->next = transactionsList.transactions;
	transactionsList.transactions = transaction;
	transactionsList.numTransactions++;
	
	return transaction;
}

/* Entry point of the program */
int main()
{
	FILE *file;
	Transaction *transaction;
	char line[1024];
	
	accountsList.head = NULL;		
	accountsList.tail = NULL;
	accountsList.numAccounts = 0;
	
	transactionsList.transactions = NULL;
	transactionsList.numTransactions = 0;
	
	/* Parse the input file and execute the commands */
	file = fopen("assignment_3_input_file.txt", "r");
	
	while(fgets(line, 1024, file))
	{
		if(line[0] == 'a')
		{
			addAccount(line);
		}
		else
		{
			/* Depositors runs first than clients, make clients wait while a depositor is running */			
			if(line[0] == 'd')
				numDepositorsRunning++;
			
			/* Execute a new transaction as a separate thread */
			transaction = addTransaction(line);
			
			/* Execute transaction on another thread */
			pthread_create(&transaction->thread, NULL, &transactionThread, transaction);
		}
	}
	
	fclose(file);
	
	/* Wait for all transactions to finish */
	transaction = transactionsList.transactions;
	
	while(transaction != NULL)
	{
		pthread_join(transaction->thread, NULL);
		transaction = transaction->next;
	}
	
	/* Report results */
	file = fopen("assignment_3_output_file.txt", "w");
	
	printf("\nEnding Balances (Written to assignment_3_output_file.txt as well:\n");
	printAccounts(stdout);
	printAccounts(file);
	
	/* Clean up */
	deleteAccounts();
	deleteTransactions();
	
	return 0;
}
