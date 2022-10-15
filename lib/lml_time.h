//
// Created by Maxence Younsi on 30/09/22.
//

#ifndef LMLOGS_LML_TIME_H
#define LMLOGS_LML_TIME_H

#include <sys/time.h>
#include <time.h>

#define LML_TIME_FUNC(timespec_ptr) clock_gettime(CLOCK_MONOTONIC, timespec_ptr)

#define LML_TIME(timespec_name, exec_code...) struct timespec timespec_name; { \
    struct timespec LML_TIME_BEFORE;              \
    struct timespec LML_TIME_AFTER;              \
    LML_TIME_FUNC(&LML_TIME_BEFORE); \
    ({exec_code});\
    LML_TIME_FUNC(&LML_TIME_AFTER); \
    timespec_subtract(&timespec_name, &LML_TIME_AFTER, &LML_TIME_BEFORE); }


/* Subtract the ‘struct timespec’ values X and Y,
   storing the result in RESULT.
   Return 1 if the difference is negative, otherwise 0. */
int timespec_subtract (struct timespec *result, struct timespec *x, struct timespec *y)
{
    /* Perform the carry for the later subtraction by updating y. */
    if (x->tv_nsec < y->tv_nsec) {
        int nsec = (y->tv_nsec - x->tv_nsec) / 1000000000 + 1;
        y->tv_nsec -= 1000000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_nsec - y->tv_nsec > 1000000000) {
        int nsec = (x->tv_nsec - y->tv_nsec) / 1000000000;
        y->tv_nsec += 1000000000 * nsec;
        y->tv_sec -= nsec;
    }

    /* Compute the time remaining to wait.
       tv_nsec is certainly positive. */
    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_nsec = x->tv_nsec - y->tv_nsec;

    /* Return 1 if result is negative. */
    return x->tv_sec < y->tv_sec;
}

#define LML_TIME_PFORMAT "%luh:%lum:%lus:%lums:%luus:%luns"
#define LML_TIME_PPARAMS(ts) (ts).tv_sec/3600, (ts).tv_sec/60, (ts).tv_sec, (ts).tv_nsec / 1000000, ((ts).tv_nsec/1000) - ((ts).tv_nsec/1000000*1000), ((ts).tv_nsec - ((ts).tv_nsec)/1000*1000)

int utc_system_timestamp(struct timespec* ts_ptr, char buf[]) {
    const int tmpsize = 21;
    struct tm tm;
    gmtime_r(&ts_ptr->tv_sec, &tm);
    strftime(buf, tmpsize, "%Y-%m-%dT%H:%M:%S.", &tm);
    sprintf(buf + tmpsize -1, "%09luZ", ts_ptr->tv_nsec);
}



#endif //LMLOGS_LML_TIME_H
