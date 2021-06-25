/**@Author: Amanpreet Gill
 * CS3307 bankaccount assignment
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
//#include <vector>

/*a structure that represents account entity*/
typedef struct Account{
	char id[5];
	char type[10];
	int balance;
	int numTransactions;
	struct  Account* next;
	pthread_mutex_t lock;
} Account;

typedef struct AccountList {
	Account* head;
	Account* tail;
	int numaccounts;
} AccountList;

/* holds jobs deposit, withdraw, or fundtransfer */
typedef struct Job {
	char type;
	Account* fromaccount;
	Account* toaccount;
	struct Job* next;
	int amount;
} Job;

/*thread struct*/
typedef struct Transaction {
	pthread_t thread;
	char id[10];
	Job* jobsHead;
	Job* jobsTail;
	struct Transaction* next;
} Transaction;

/* Holds the list of transactions */
typedef struct TransactionsList {
	Transaction* transactions;
	int numTransactions;
} TransactionsList;
//Structs complete--------------------------------------------------------------------------------

int numDepositorsRunning = 0;
int numDepositorsFinished = 0;
AccountList accList;
TransactionsList transactionsList;
pthread_mutex_t transferFundsLock = PTHREAD_MUTEX_INITIALIZER;

//Global variables----------------------------------------------------------------------------------
/* lock that locks permissions of clients until after deposits done */
pthread_mutex_t Lock = PTHREAD_MUTEX_INITIALIZER; pthread_cond_t Wait = PTHREAD_COND_INITIALIZER;

/* Create new account into list*/
void addaccount(char* l) {
	Account* account;
	char* token;

	account = (Account*)malloc(sizeof(Account));
	account->next = NULL;
	account->balance = 0;
	account->numTransactions = 0;

	pthread_mutex_init(&account->lock, NULL);
	token = strtok(l, " ");
	strcpy(account->id, token);

	token = strtok(NULL, " ");
	while (token != NULL) {
		if (strcmp(token, "type") == 0)
		{
			/* Extract the account type */
			token = strtok(NULL, " ");
			strcpy(account->type, token);
		}

		token = strtok(NULL, " ");
	}

	/* Add account to the list */
	if (accList.numaccounts == 0)
	{
		accList.head = account;
		accList.tail = account;
	}
	else
	{
		accList.tail->next = account;
		accList.tail = account;
	}

	accList.numaccounts++;
}

/* Find the account object that holds the ID */
Account* findaccount(char* id) {
	Account* current;

	current = accList.head;

	while (current != NULL)
	{
		if (strcmp(current->id, id) == 0)
			return current;

		current = current->next;
	}

	return NULL;
}

/* Delete all jobs */
void deleteJobs(Job* jobs) {
	Job* next;
	Job* current;

	next = NULL;
	current = jobs;

	while (current != NULL)
	{
		next = current->next;
		free(current);
		current = next;
	}
}

/* Delete all transactions */
void deleteTransactions() {
	Transaction* next;
	Transaction* current;

	next = NULL;
	current = transactionsList.transactions;

	while (current != NULL){
		next = current->next;
		deleteJobs(current->jobsHead);
		free(current);
		current = next;
	
}
}


void printaccounts(FILE* outFile) {
	Account* curr;

	curr = accList.head;

	while (curr != NULL)
	{
		fprintf(outFile, "%s type %s %d\n",
			curr->id,
			curr->type,
			curr->balance);
		curr = curr->next;
	
	}
}

void depositToaccount(Account* account, int amount) {
	pthread_mutex_lock(&account->lock);
	int zero = 0;
	printf("Depositing $%d to account %s with starting balance of $%d\n", amount, account->id, account->balance);

	account->balance += amount + zero;

	account->numTransactions++;
	printf("    Ending balance of $%d\n", account->balance);
	printf("\n");

	pthread_mutex_unlock(&account->lock);
}

/* Withdraw from account */
void withdrawFromaccount(Account* account, int amount) {

	pthread_mutex_lock(&account->lock);

	printf("Withdrawing $%d from account %s with starting balance of $%d\n", amount, account->id, account->balance);

	/* Check balance... */
	if (account->balance >= amount)
	{
		/* Safe side... there's enough balance to withdraw */
		account->balance -= amount;
		account->numTransactions++;
	}
	else
	{
		printf("    Withdrawal rejected, amount cannot continue \n");
		printf("        because of insufficient balance and account not overdraft protected\n");
	}

	printf("    Ending balance of $%d\n", account->balance);
	printf("\n");

	pthread_mutex_unlock(&account->lock);
}

// Transfer funds
void transferFundsFromAndToaccount(Account* fromaccount, Account* toaccount, int amount) {

	pthread_mutex_lock(&transferFundsLock);
	pthread_mutex_lock(&fromaccount->lock);
	pthread_mutex_lock(&toaccount->lock);

	printf("Transferring $%d from %s to %s\n", amount, fromaccount->id, toaccount->id);
	printf("    account (Sender) %s has starting balance of $%d\n", fromaccount->id, fromaccount->balance);
	printf("    account (Receiver) %s has starting balance of $%d\n", toaccount->id, toaccount->balance);

	printf("    account %s has ending balance of $%d\n", fromaccount->id, fromaccount->balance);
	printf("    account %s has ending balance of $%d\n", toaccount->id, toaccount->balance);
	printf("\n");

	pthread_mutex_unlock(&toaccount->lock);
	pthread_mutex_unlock(&fromaccount->lock);
	pthread_mutex_unlock(&transferFundsLock);
}

/* method runs threads in parallel */
void* transactionThread(void* args) {
	Transaction* transaction;
	Job* currentJob;
	transaction = (Transaction*)args;
	if (transaction->id[0] == 'c')
	{
		/* Clients have to wait for all depositors to finish before continuing */
		pthread_mutex_lock(&Lock);

		while (numDepositorsFinished < numDepositorsRunning)
			pthread_cond_wait(&Wait, &Lock);

		pthread_cond_signal(&Wait);
		pthread_mutex_unlock(&Lock);
	}

	printf("%s thread is running...\n", transaction->id);

	/* Do all sequence of transaction */
	currentJob = transaction->jobsHead;

	while (currentJob != NULL)
	{
		if (currentJob->type == 'd')
		{
			/* Perform a deposit on an account */
			printf("%s deposit $%d to account %s\n", transaction->id, currentJob->amount, currentJob->fromaccount->id);

			/* apply only to clients and not to depositors */
			if (transaction->id[0] == 'd')
				depositToaccount(currentJob->fromaccount, currentJob->amount);
			else
				depositToaccount(currentJob->fromaccount, currentJob->amount);
		}
		else if (currentJob->type == 'w')
		{
			/* Peform a withdrawal on an account */
			printf("%s withdraw $%d from account %s\n", transaction->id, currentJob->amount, currentJob->fromaccount->id);

			withdrawFromaccount(currentJob->fromaccount, currentJob->amount);
		}
		else if (currentJob->type == 't')
		{
			/* Perform a fund transferfrom one account to another */
			printf("%s transfers $%d from account %s to account %s\n", transaction->id,
				currentJob->amount, currentJob->fromaccount->id, currentJob->toaccount->id);

			transferFundsFromAndToaccount(currentJob->fromaccount, currentJob->toaccount, currentJob->amount);
		}

		currentJob = currentJob->next;
	}

	/* A depositor updates when it is done and signals clients if its ready for them to go */
	if (transaction->id[0] == 'd')
	{
		numDepositorsFinished++;
		pthread_cond_signal(&Wait);
		pthread_mutex_unlock(&Lock);
	}

	printf("%s finished...\n", transaction->id);

	return (void*)NULL;
}
//create transaction
Transaction* addTransaction(char* line) {
	Transaction* transaction;
	char* token;
	Job* job;

	transaction = (Transaction*)malloc(sizeof(Transaction));
	transaction->id[0] = '\0';
	transaction->jobsHead = NULL;
	transaction->jobsTail = NULL;
	transaction->next = NULL;

	/* Extract the ID */
	token = strtok(line, " ");
	strcpy(transaction->id, token);

	/* Extract the jobs */
	token = strtok(NULL, " ");

	while (token != NULL){ 
		job = (Job*)malloc(sizeof(Job));
		job->type = token[0];
		job->fromaccount = NULL;
		job->toaccount = NULL;
		job->next = NULL;

		if (job->type == 'd' || job->type == 'w') {
			/* Create a deposit job */
			token = strtok(NULL, " ");
			job->fromaccount = findaccount(token);

			token = strtok(NULL, " ");
			sscanf(token, "%d", &job->amount);
		}
		else if (job->type == 't') {
			/* Create a withdraw job */
			token = strtok(NULL, " ");
			job->fromaccount = findaccount(token);

			token = strtok(NULL, " ");
			job->toaccount = findaccount(token);

			token = strtok(NULL, " ");
			sscanf(token, "%d", &job->amount);
		}

		if (transaction->jobsHead == NULL) {
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

	return transaction;
}

int main() {
	FILE* file;
	Transaction* transaction;
	char line[1050];

	accList.head = NULL;
	accList.tail = NULL;
	accList.numaccounts = 0;

	transactionsList.transactions = NULL;
	transactionsList.numTransactions = 0;

	file = fopen("assignment_6_input_file.txt", "r");

	while (fgets(line, 1024, file))
	{
		if (line[0] == 'a')
		{
			addaccount(line);
		}
		else
		{
			// Depositors runs first than clients, make clients wait while a depositor is running
			if (line[0] == 'd')
				numDepositorsRunning++;

			// Execute a new transaction as a separate thread
			transaction = addTransaction(line);

			// Execute transaction on another thread
			pthread_create(&transaction->thread, NULL, &transactionThread, transaction);
		}
	}

	fclose(file);

	// Wait before transactions finish
	transaction = transactionsList.transactions;

	while (transaction != NULL)
	{
		pthread_join(transaction->thread, NULL);
		transaction = transaction->next;
	}

	// Report results
	file = fopen("assignment_6_output_file.txt", "w");
	printaccounts(stdout);
	deleteTransactions();

	return 0;
}
