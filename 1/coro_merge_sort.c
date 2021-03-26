/* Vlasov Roman */
/* Попытался решить на 20 баллов. Опоздал на 1 день */


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <ucontext.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>


#define ERROR(...) 	fprintf(stderr, __VA_ARGS__); \
					exit(EXIT_FAILURE);
#define OUTPUT(...)	fprintf( __VA_ARGS__)
#define stack_size 	32 * 1024 * 1024   				// 32 MB


typedef struct
{
	char* filename;
	int* array;
	int array_size;
} sorted_element;


sorted_element** se;							// global struct described coroutines's data


int N_COROUTINES;							// amount of coroutines
int READY_COROUTINES = 0;					// amount of coroutines that waited
char** stacks;								// array of stack (coroutines and sched stack)


ucontext_t* cntx;							// array with contexts of coroutines
ucontext_t* cntx_main; 						// main context (before and after "co-routining")
ucontext_t* cntx_sched;						// context of scheduler (where i will handle signals)


// time
clock_t prev_time;
clock_t cur_time;
clock_t* time_acc;
double tick;								// one tick (T / N) in milliseconds
int* counts;								// surrendered controls


int 
check_time()
{
	cur_time = clock();
	if ((cur_time - prev_time) * 1000 / (CLOCKS_PER_SEC) >= tick) // microseconds in tick
		return 1;
	return 0;
}


#define TIME_FLUSH(id)				time_acc[id] += cur_time - prev_time; counts[id]++;

#define SWAPCONTEXT_SCHED(id)		prev_time = clock(); \
									if (swapcontext(cntx_sched, \
										&cntx[((id + 1) % N_COROUTINES)]) == -1) {\
										ERROR("swapcontext [sched-coro] error\n");\
									}

#define SWAPCONTEXT_CORO_FORCE(id) 	if (swapcontext(&cntx[id], \
										cntx_sched) == -1) { \
										ERROR("swapcontext [coro-sched] error\n"); \
									}
#define SWAPCONTEXT_CORO(id) 		if (check_time()) { \
										TIME_FLUSH(id);	\
										SWAPCONTEXT_CORO_FORCE(id); \
									}

#define SWAPCONTEXT(c1, c2)			if (swapcontext(c1, \
										c2) == -1) { \
										ERROR("swapcontext [main] error\n"); \
									}


int
write_file(sorted_element** se, int size, char* filename)
{
	FILE *fp = fopen(filename, "w");
	if (fp == NULL)
	{
		ERROR("failed to open file %s\n", filename);
		return -1;
	}

	int idxs[size];
	for (int i = 0; i < size; i++) 
		idxs[i] = 0;
	
	int total_numb = 0;
	for (int i = 0; i < size; i++) 
		total_numb += se[i]->array_size;
	
	for (int _ = 0; _ < total_numb; _++)	
	{
		int min_i = 0;

		for (int i = 0; i < size; i++) 
		{
			if (idxs[i] < se[i]->array_size)
			{
				min_i = i;
				break;
			}
		}
		int min = se[min_i]->array[idxs[min_i]];
		for (int j = min_i; j < size; j++) 
		{
			if (idxs[j] < se[j]->array_size)
			{
				if (se[j]->array[idxs[j]] < min) 
				{	
					min_i = j;
					min = se[min_i]->array[idxs[min_i]];
				}
			}
		}
		OUTPUT(fp, "%d\n", se[min_i]->array[idxs[min_i]]);
		idxs[min_i]++;
	}
	fclose(fp);
	return 0;
}


sorted_element*
read_file(char* filename, int id)
{
	// i will multiply by 2 this value every time when 
	// array seems small
	int array_size = 32;
	SWAPCONTEXT_CORO(id);
	int* array = (int*) malloc(sizeof(int) * array_size);
	SWAPCONTEXT_CORO(id);
	if (!array) 
	{
		ERROR("malloc error\n");	
	}
	SWAPCONTEXT_CORO(id);

	FILE *fp = fopen(filename, "r");
	SWAPCONTEXT_CORO(id);
	if (fp == NULL)
	{
		ERROR("failed to open file %s\n", filename);
		return NULL;
	}
	int idx = 0;
	SWAPCONTEXT_CORO(id);
	while (fscanf(fp, "%d", &array[idx++]) != EOF)
	{
		SWAPCONTEXT_CORO(id);
		if (idx >= array_size)
		{
			array_size *= 2;	
			SWAPCONTEXT_CORO(id);
			array = (int*) realloc(array, sizeof(int) * array_size);
			SWAPCONTEXT_CORO(id);
			if (!array)
			{
				ERROR("malloc failed\n");
			}
		}
		SWAPCONTEXT_CORO(id);
	}
	fclose(fp);
	sorted_element* s = (sorted_element*) malloc(sizeof(sorted_element));
	SWAPCONTEXT_CORO(id);
	if (!s) 
	{
		ERROR("malloc failed\n");
	}
	SWAPCONTEXT_CORO(id);
	s->filename = filename;
	SWAPCONTEXT_CORO(id);
	s->array = array;
	SWAPCONTEXT_CORO(id);
	s->array_size = idx - 1;
	return s;
}


// helper function that merging two arrays into one by pivot element in mid
void 
__merge(int *array, int left, int mid, int right, int id) 
{
	int n1 = mid - left + 1;
	SWAPCONTEXT_CORO(id);
	int n2 = right - mid; 
	SWAPCONTEXT_CORO(id);
    int L[n1], R[n1];
	SWAPCONTEXT_CORO(id);
    for (int i = 0; i < n1; i++)
	{
		L[i] = array[left + i];
		SWAPCONTEXT_CORO(id);
	}
	SWAPCONTEXT_CORO(id);
	for (int j = 0; j < n2; j++)
	{
		R[j] = array[mid + j + 1];
		SWAPCONTEXT_CORO(id);
	}
	SWAPCONTEXT_CORO(id);
	int i = 0, j = 0, k = left;
	SWAPCONTEXT_CORO(id);
	while (i < n1 && j < n2)
	{
		SWAPCONTEXT_CORO(id);
		if (L[i] <= R[j])
		{
			array[k++] = L[i++];
			SWAPCONTEXT_CORO(id);
		}
		else
		{
			array[k++] = R[j++];
			SWAPCONTEXT_CORO(id);
		}
	}
	SWAPCONTEXT_CORO(id);
	while (i < n1)
	{
		array[k++] = L[i++];
		SWAPCONTEXT_CORO(id);
	}
	SWAPCONTEXT_CORO(id);
	while (j < n2)
	{
		array[k++] = R[j++];
		SWAPCONTEXT_CORO(id);
	}
	SWAPCONTEXT_CORO(id);
}


void 
merge_sort(int* array, int left, int right, int id)
{
	if (left < right)
	{
		int mid = (int) ((right + left) / 2);
		SWAPCONTEXT_CORO(id);
		merge_sort(array, left, mid, id);
		SWAPCONTEXT_CORO(id);
		merge_sort(array, mid + 1, right, id);
		SWAPCONTEXT_CORO(id);
		__merge(array, left, mid, right, id);
		SWAPCONTEXT_CORO(id);
	}
	return;
}


void* 
coroutine_merge(int id, char* filename)
{
	se[id] = read_file(filename, id);
	SWAPCONTEXT_CORO(id);
	merge_sort(se[id]->array, 0, se[id]->array_size - 1, id);	
	SWAPCONTEXT_CORO(id);
	TIME_FLUSH(id);

	// wait while all coroutines did not stop their tasks
	// kind of sync
	READY_COROUTINES += 1;	
	while (READY_COROUTINES != N_COROUTINES)
	{
		SWAPCONTEXT_CORO_FORCE(id);
	}
	SWAPCONTEXT(&cntx[id], cntx_main);
}


// main scheduler of coroutines
void
coroutine_sched()
{
	int id_coro = 0;
	while (1) 
	{
		SWAPCONTEXT_SCHED(id_coro);
		id_coro = (id_coro + 1) % N_COROUTINES; // RR
	}
}


// stack allocation
void*
allocate_stack()
{
	void* stack = (void*) malloc(stack_size);
	mprotect(stack, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC);
	return stack;
}


void
deallocate_stacks()
{
	for (int i = 0; i < N_COROUTINES + 1; i++)
		free(stacks[i]);
	free(stacks);
}


// init global variables related to time handling
void 
time_setup(int T, int N)
{
	time_acc = (clock_t*) malloc(sizeof(clock_t) * N);
	for (int i = 0; i < N; i++) 
		time_acc[i] = 0;
	tick = T / N;
	counts = (int*) malloc(sizeof(int) * N);
}	


void 
time_destroy()
{
	free(time_acc);	
	free(counts);
}


void
init_context(ucontext_t* ctx, char* stack, int stacksize)
{
	if (getcontext(ctx) == -1)
	{
		ERROR("getcontext error\n");
	}
	ctx->uc_stack.ss_sp = stack;
	ctx->uc_stack.ss_size = stack_size;
	ctx->uc_link = NULL;
}


void
context_destroy()
{
	free(cntx);
	free(cntx_main);
	free(cntx_sched);
}


void
destroy_sort_elements(sorted_element** sa)
{
	for (int i = 0; i < N_COROUTINES; i++)
	{
		free(sa[i]->array);
		free(sa[i]);
	}
	free(sa);
}


int
main(int argc, char** argv)
{
	if (argc < 3) 
	{
		printf("Please usage: ./main [latency : milliseconds (10^-3)] [files]\n");
		return 0;
	}

	// amount of coroutines	
	N_COROUTINES = argc - 1 - 1; // [0] - prog's name, [1] - T

	// array of stacks
	stacks = (char**) malloc(sizeof(char*) * (argc - 1));
	if (!stacks)
	{
		ERROR("malloc failed\n");
	}
	for (int i = 0; i < N_COROUTINES + 1; i++) // +1 because add sched's stack
		stacks[i] = (char*) allocate_stack();
	
	// allocate array of contexts
	cntx = (ucontext_t*) malloc(sizeof(ucontext_t) * (argc - 2));
	if (!cntx)
	{
		ERROR("malloc failed\n");
	}

	// initialization of coroutines' contexts
	for (int i = 0; i < N_COROUTINES; i++) 
	{
		init_context(&cntx[i], stacks[i], stack_size);
		makecontext(&cntx[i], coroutine_merge, 2, i, argv[i + 2], NULL);
	}

	// initialization of scheduler's context
	cntx_main = (ucontext_t*) malloc(sizeof(ucontext_t));
	if (!cntx_main)
	{
		ERROR("malloc failed\n");
	}
	cntx_sched = (ucontext_t*) malloc(sizeof(ucontext_t));
	if (!cntx_sched)
	{
		ERROR("malloc failed\n");
	}
	init_context(cntx_sched, stacks[N_COROUTINES], stack_size);
	makecontext(cntx_sched, coroutine_sched, 0);

	// setup time handling
	time_setup(atoi(argv[1]), N_COROUTINES); // argv[1] - milliseconds

	// create array with states (files)
	se = (sorted_element**) malloc(sizeof(sorted_element*) * (N_COROUTINES));
	if (!se)
	{
		ERROR("malloc failed\n");
	}

	// start 'corouting'
	SWAPCONTEXT(cntx_main, cntx_sched);

	// output
	write_file(se, N_COROUTINES, "output.txt"); 
	
	// time printing
	for (int i = 0; i < N_COROUTINES; i++)
		OUTPUT(stdout, "coroutine [%d] : total time = [%ld] microseconds (10^-6 s), surrendered control = [%d]\n", i, \
				((long int)time_acc[i]) * 1000000 / (CLOCKS_PER_SEC), \
				(counts[i]));

	// freeing memory
	time_destroy();
	deallocate_stacks();
	context_destroy();
	destroy_sort_elements(se);
	
	return 0;
}	
