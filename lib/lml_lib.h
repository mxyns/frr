//
// Created by Maxence Younsi on 07/10/22.
//

#ifndef LMLOGS_LML_LIB_H
#define LMLOGS_LML_LIB_H

#define LML_COND(bool, ...) LML_COND_##bool(__VA_ARGS__)
#define LML_COND_0(...)
#define LML_COND_1(...) __VA_ARGS__

#define LML_PREV_OPT 1
#define LML_RECORD_MEMSIZE_OPT 1
#define LML_CLEAR_OPT 1
#define LML_NO_OPT 0

#define LML_DECLARE_ALL(PREFIX, TYPE, PREV_OPT, MEMSIZE_OPT, CLEARABLE_OPT)   \
    TYPE;                                                                      \
    struct PREFIX##_stack;                                                     \
    struct PREFIX##_log;                                                       \
    struct PREFIX##_stack *PREFIX##_stack_new(                                 \
            size_t stack_cap LML_COND(MEMSIZE_OPT, , size_t *alloc_size_ref)); \
    struct PREFIX##_log *PREFIX##_log_new(size_t stack_cap);                   \
    struct PREFIX##_stack *PREFIX##_log_expand(struct PREFIX##_log *log);      \
    TYPE *PREFIX##_log_push(struct PREFIX##_log *log, TYPE entry);             \
    void PREFIX##_log_dump(struct PREFIX##_log *log,                           \
                           void (*consumer)(struct PREFIX##_log * log,         \
                                            struct PREFIX##_stack * stack,     \
                                            size_t index, TYPE * entry,        \
                                            void *extra),                      \
                           void *extra);                                       \
    void PREFIX##_log_free(struct PREFIX##_log *log);                          \
    LML_COND(CLEARABLE_OPT, void PREFIX##_log_clear(struct PREFIX##_log *log));


#define LML_DEFINE_TYPES(PREFIX, TYPE, PREV_OPT, MEMSIZE_OPT, CLEARABLE_OPT,   \
                         FIELDS...)                                            \
    TYPE FIELDS;                                                               \
    struct PREFIX##_stack {                                                    \
        size_t stack_cap;                                                      \
        size_t stack_size;                                                     \
        TYPE *entries;                                                         \
        struct PREFIX##_stack *next;                                           \
        LML_COND(PREV_OPT, struct PREFIX##_stack *prev;)                       \
    };                                                                         \
                                                                               \
    struct PREFIX##_log {                                                      \
        LML_COND(MEMSIZE_OPT, size_t alloc_size;)                              \
        size_t stack_count;                                                    \
        struct PREFIX##_stack *head;                                           \
        LML_COND(CLEARABLE_OPT, struct PREFIX##_stack *curr;)                  \
        struct PREFIX##_stack *tail;                                           \
    };


#define LML_DEFINE_FUNCS(PREFIX, TYPE, PREV_OPT, MEMSIZE_OPT, CLEARABLE_OPT)   \
                                                                               \
    struct PREFIX##_stack *PREFIX##_stack_new(size_t stack_cap LML_COND(       \
            MEMSIZE_OPT, , size_t *alloc_size_ref)) {                          \
                                                                               \
        size_t alloc_size =                                                    \
                sizeof(struct PREFIX##_stack) + sizeof(TYPE) * stack_cap;      \
        struct PREFIX##_stack *stack = malloc(alloc_size);                     \
        stack->stack_cap = stack_cap;                                          \
        stack->stack_size = 0;                                                 \
        stack->entries = (TYPE *) (stack + 1);                                 \
        stack->next = NULL;                                                    \
        LML_COND(PREV_OPT, stack->prev = NULL;)                                \
        LML_COND(MEMSIZE_OPT,                                                  \
                 if (alloc_size_ref) *alloc_size_ref = alloc_size;)            \
                                                                               \
        return stack;                                                          \
    }                                                                          \
                                                                               \
    struct PREFIX##_log *PREFIX##_log_new(size_t stack_cap) {                  \
                                                                               \
        size_t alloc_size = sizeof(struct PREFIX##_log) +                      \
                            sizeof(struct PREFIX##_stack) +                    \
                            sizeof(TYPE) * stack_cap;                          \
        struct PREFIX##_log *log = malloc(alloc_size);                         \
        struct PREFIX##_stack *stack = (struct PREFIX##_stack *) (log + 1);    \
        stack->stack_cap = stack_cap;                                          \
        stack->stack_size = 0;                                                 \
        stack->entries = (TYPE *) (stack + 1);                                 \
        stack->next = NULL;                                                    \
        LML_COND(PREV_OPT, stack->prev = NULL;)                                \
        LML_COND(MEMSIZE_OPT, log->alloc_size = alloc_size;)                   \
        log->stack_count = 1;                                                  \
        log->head = stack;                                                     \
        log->tail = stack;                                                     \
        LML_COND(CLEARABLE_OPT, log->curr = stack;)                            \
                                                                               \
        return log;                                                            \
    }                                                                          \
                                                                               \
    struct PREFIX##_stack *PREFIX##_log_expand(struct PREFIX##_log *log) {     \
                                                                               \
        LML_COND(MEMSIZE_OPT, size_t alloc_size = 0;)                          \
        struct PREFIX##_stack *stack = PREFIX##_stack_new(                     \
                log->tail->stack_cap LML_COND(MEMSIZE_OPT, , &alloc_size));    \
        log->tail->next = stack;                                               \
        LML_COND(PREV_OPT, stack->prev = log->tail;)                           \
        log->tail = stack;                                                     \
        log->stack_count++;                                                    \
        LML_COND(MEMSIZE_OPT, log->alloc_size += alloc_size;)                  \
        return log->tail;                                                      \
    }                                                                          \
                                                                               \
    TYPE *PREFIX##_log_push(struct PREFIX##_log *log, TYPE entry) {            \
                                                                               \
        struct PREFIX##_stack *stack = log->tail;                              \
        LML_COND(CLEARABLE_OPT, stack = log->curr;)                            \
        if (stack->stack_size == stack->stack_cap) {                           \
            stack = LML_COND(CLEARABLE_OPT,                                    \
                             stack->next != NULL ? stack->next :)              \
                    PREFIX##_log_expand(log);                                  \
        }                                                                      \
                                                                               \
        LML_COND(CLEARABLE_OPT, log->curr = stack;)                            \
        stack->entries[stack->stack_size++] = entry;                           \
        return &stack->entries[stack->stack_size - 1];                         \
    }                                                                          \
                                                                               \
    void PREFIX##_log_dump(struct PREFIX##_log *log,                           \
                           void (*consumer)(struct PREFIX##_log * log,         \
                                            struct PREFIX##_stack * stack,     \
                                            size_t index, TYPE * entry,        \
                                            void *extra),                      \
                           void *extra) {                                      \
                                                                               \
        struct PREFIX##_stack *stack = log->head;                              \
        do {                                                                   \
            for (size_t i = 0; i < stack->stack_size; ++i) {                   \
                consumer(log, stack, i, &stack->entries[i], extra);            \
            }                                                                  \
                                                                               \
            LML_COND(CLEARABLE_OPT, /* early break if the stack isn't full */  \
                     if (stack == log->curr ||                                 \
                         stack->stack_size < stack->stack_cap) break;);        \
                                                                               \
            stack = stack->next;                                               \
        } while (stack != NULL);                                               \
    }                                                                          \
                                                                               \
    void PREFIX##_log_free(struct PREFIX##_log *log) {                         \
                                                                               \
        struct PREFIX##_stack *stack = log->head->next;                        \
        struct PREFIX##_stack *to_free;                                        \
        while (stack != NULL) {                                                \
            to_free = stack;                                                   \
            stack = stack->next;                                               \
            free(to_free);                                                     \
        }                                                                      \
                                                                               \
        free(log);                                                             \
    }                                                                          \
                                                                               \
    LML_COND(                                                                  \
            CLEARABLE_OPT, void PREFIX##_log_clear(struct PREFIX##_log *log) { \
                struct PREFIX##_stack *stack = log->head;                      \
                do { /* we know log->head will not be null */                  \
                    stack->stack_size = 0;                                     \
                    stack = stack->next;                                       \
                } while (stack != NULL);                                       \
                log->curr = log->head;                                         \
            })


#endif//LMLOGS_LML_LIB_H
