#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/sysinfo.h>    /* get_nprocs() */
#include "axl_internal.h"
#include "kvtree_util.h"
#include "axl_sync.h"

/* get_nprocs() */
#if defined(__APPLE__)
#include <sys/sysctl.h>
#else
#include <sys/sysinfo.h>
#endif

#define MIN(a,b) (a < b ? a : b)

/*
 *  We default our number of threads to the lesser of:
 *
 *  - The number of CPU threads
 *  - MAX_THREADS
 *  - The number of files being transfered
 */
#define MAX_THREADS 16  /* We don't see much scaling past 16 threads */

struct axl_work
{
    struct axl_work *next;
    /* The file element in the kvtree */
    kvtree_elem* elem;
};

struct axl_pthread_data
{
    /* Lock to protect workqueue */
    pthread_mutex_t lock;

    /* Our workqueue */
    struct axl_work *head;
    struct axl_work *tail;

    /* Number of threads */
    unsigned int threads;

    /* Array of our thread IDs */
    pthread_t *tid;
};

/* Linux and OSX compatible 'get number of hardware threads' */
unsigned int axl_get_nprocs(void)
{
    unsigned int cpu_threads;
#if defined(__APPLE__)
    int count;
    size_t size = sizeof(count);
    if (sysctlbyname("hw.ncpu", &count, &size, NULL, 0)) {
        cpu_threads = 1;
    } else {
        cpu_threads = count;
    }
#else
    cpu_threads = get_nprocs();
#endif
    return cpu_threads;
}

/* The actual pthread function */
static void *
axl_pthread_func(void *arg)
{
    struct axl_pthread_data *pdata = arg;
    struct axl_work *work;
    int rc;
    char *src, *dst;
    kvtree_elem *elem;
    kvtree *elem_hash;

    while (1) {
        pthread_mutex_lock(&pdata->lock);

        /* Get a file to transfer */
        work = pdata->head;
        if (!work) {
            /* No more work to do */
            pthread_mutex_unlock(&pdata->lock);
            pthread_exit((void *) AXL_SUCCESS);
        } else {
            /* Take our work out of the queue */
            pdata->head = pdata->head->next;
            pthread_mutex_unlock(&pdata->lock);
        }

        /* Lookup our filename */
        elem = work->elem;
        elem_hash = kvtree_elem_hash(elem);
        src = kvtree_elem_key(elem);
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &dst);

        rc = axl_file_copy(src, dst, axl_file_buf_size, NULL);
        AXL_DBG(2, "%s: Read and copied %s to %s, rc %d",
            __func__, src, dst, rc);

        /* Record the success/failure of the individual file transfer */
        if (rc == AXL_SUCCESS) {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_DEST);
        } else {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_ERROR);
        }

        free(work);

        if (rc != AXL_SUCCESS)
            pthread_exit((void *) AXL_FAILURE);
    }
    return NULL;
}

static struct axl_pthread_data *
axl_pthread_create(unsigned int threads)
{
    struct axl_pthread_data *pdata;

    pdata = calloc(1, sizeof(*pdata));
    if (!pdata)
        return NULL;

    if (pthread_mutex_init(&pdata->lock, NULL) != 0) {
        free(pdata);
        return NULL;
    }

    pdata->tid = calloc(threads, sizeof(pdata->tid[0]));
    if (!pdata->tid) {
        free(pdata);
        return NULL;
    }
    pdata->threads = threads;

    pdata->head = pdata->tail = NULL;

    return pdata;
}

/* Free up our axl_pthread_data.  We assume no one is competing for the lock. */
static void
axl_pthread_free_pdata(struct axl_pthread_data *pdata)
{
    struct axl_work *work;

    /*
     * Free our workqueue item first (if any).  If our transfer completed
     * successfully, all workqueue entries would have already been freed.
     */
    work = pdata->head;
    while (pdata->head) {
        work = pdata->head->next;
        free(pdata->head);
        pdata->head = work;
    }

    free(pdata->tid);
    free(pdata);
}

/* Add a file to our workqueue.  We assume the lock is already held */
static int
axl_pthread_add_work(struct axl_pthread_data *pdata, kvtree_elem* elem)
{
    struct axl_work *work;

    work = calloc(1, sizeof(*work));
    if (!work)
        return AXL_FAILURE;

    work->elem = elem;

    if (!pdata->head) {
         /* First time we inserted into the workqueue */
        pdata->head = work;
        pdata->tail = work;
    } else {
        /* Insert it at the end of the list */
        pdata->tail->next = work;
        pdata->tail = work;
    }
    return AXL_SUCCESS;
}

/*
 * Create and wakeup all our threads to start transferring the files in
 * the workqueue.
 */
static int
axl_pthread_run(struct axl_pthread_data *pdata)
{
    int i;
    int rc;
    int final_rc = AXL_SUCCESS;

    for (i = 0; i < pdata->threads; i++) {
        rc = pthread_create(&pdata->tid[i], NULL, &axl_pthread_func, pdata);
        if (rc != 0)
            return rc;
    }

    return final_rc;
}

int axl_pthread_start (int id)
{
    /* assume we'll succeed */
    int rc = AXL_SUCCESS;
    struct axl_pthread_data *pdata;
    unsigned int file_count, threads, cpu_threads;

    /* get pointer to file list for this dataset */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* mark dataset as in progress */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);

    kvtree_elem* elem = NULL;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    file_count = kvtree_size(files);

    /* Get number of hardware threads */
    cpu_threads = axl_get_nprocs();

    threads = MIN(cpu_threads, MIN(file_count, MAX_THREADS));

    /* Create the data structure for our threads */
    pdata = axl_pthread_create(threads);
    if (!pdata)
        return AXL_FAILURE;

    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        /* get the hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);

        /* WEIRD case: we've restarted a sync transfer that was going */
        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);
        if (status == AXL_STATUS_DEST) {
            /* this file was already transfered */
            continue;
        }

        rc = axl_pthread_add_work(pdata, elem);
        if (rc != AXL_SUCCESS) {
            printf("something bad happened\n");
            /*
             * Something bad happened.  Break here instead of returning so
             * that pdata gets freed (at the end of this function).
             */
            break;
        }
    }

    /*
     * Store our pointer to our pthread data in our kvtree.  KVTree doesn't
     * doesn't support pointers, so we store it as a int64_t.  */
    rc = kvtree_util_set_int64(file_list, AXL_KEY_PTHREAD_DATA, (int64_t) pdata);
    if (rc != AXL_SUCCESS) {
        AXL_ERR("Couldn't set AXL_KEY_PTHREAD_DATA");
        return rc;
    }

    /*
     * At this point, all our files are queued in the workqueue.  Start the
     * transfers.
     */
    rc = axl_pthread_run(pdata);
    if (rc == AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);
    } else {
        AXL_ERR("Couldn't spawn all threads");
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_ERROR);
    }

    return rc;
}

int axl_pthread_test (int id)
{
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    int64_t ptr;
    struct axl_pthread_data *pdata;

    kvtree_util_get_int64(file_list, AXL_KEY_PTHREAD_DATA, &ptr);
    pdata = (struct axl_pthread_data *) ptr;

    /* Is there still work in the workqueue? */
    if (pdata->head) {
        /* Yes there is, we need to wait */
        return AXL_FAILURE;
    }

    return AXL_SUCCESS;
}

int axl_pthread_wait (int id)
{
    /* get pointer to file list for this dataset */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    int64_t ptr;
    struct axl_pthread_data *pdata;
    int rc;
    int final_rc = AXL_SUCCESS;
    int i;
    void *rc_ptr;

    kvtree_util_get_int64(file_list, AXL_KEY_PTHREAD_DATA, &ptr);
    pdata = (struct axl_pthread_data *) ptr;
    if (!pdata) {
        /* Did they call AXL_Cancel() and then AXL_Wait()? */
        AXL_ERR("No pthread data\n");
        return AXL_FAILURE;
    }

    /* All our threads are now started.  Wait for them to finish */
    for (i = 0; i < pdata->threads; i++) {
        rc = pthread_join(pdata->tid[i], &rc_ptr);

        /*
         * Thread either finished successfully, or was canceled by
         * AXL_Cancel().  Both statuses are valid.
         */
        if (rc != 0 && rc_ptr != PTHREAD_CANCELED) {
                AXL_ERR("pthread_join(%d) failed (%d)", i, rc);
                return AXL_FAILURE;
        }

        /*
         * Check the rc that the thread actually reported.  The thread
         * returns a void * that we encode our rc value in.
         */
        rc = (int) ((unsigned long) rc_ptr);
        if (rc) {
            final_rc |= AXL_FAILURE;
        }
    }
    if (final_rc != AXL_SUCCESS) {
        AXL_ERR("Couldn't join all threads");
        return AXL_FAILURE;
    }

    kvtree_util_set_int64(file_list, AXL_KEY_PTHREAD_DATA, 0);
    axl_pthread_free_pdata(pdata);

    /* All threads are now successfully finished */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);

    return axl_sync_wait(id);
}

int axl_pthread_cancel (int id)
{
    /* get pointer to file list for this dataset */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    int64_t ptr;
    struct axl_pthread_data *pdata;
    int rc = 0;
    int i;

    kvtree_util_get_int64(file_list, AXL_KEY_PTHREAD_DATA, &ptr);
    pdata = (struct axl_pthread_data *) ptr;

    for (i = 0; i < pdata->threads; i++) {
        rc |= pthread_cancel(pdata->tid[i]);
    }
    if (rc != 0) {
        AXL_ERR("Bad return code from pthread_cancel()\n");
    }
    return AXL_FAILURE;
}

void axl_pthread_free (int id)
{
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    int64_t ptr;
    struct axl_pthread_data *pdata;

    /*
     * pdata should have been freed in AXL_Wait(), but maybe they just did
     * an AXL_Cancel() and then an AXL_Free().  If so, pdata will be set
     * and we should free it.
     */
    kvtree_util_get_int64(file_list, AXL_KEY_PTHREAD_DATA, &ptr);
    pdata = (struct axl_pthread_data *) ptr;
    if (pdata) {
        axl_pthread_free_pdata(pdata);
    }
}
