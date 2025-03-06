#lang racket/base

(require actor
         racket/match)

(provide
 exn:fail:pool?
 current-idle-timeout-slack
 pool abandon lease release close oops)

(define-logger resource-pool)

(define current-idle-timeout-slack
  (make-parameter (* 15 1000)))

(struct exn:fail:pool exn:fail ())
(struct state (stopped? total idle busy waiters deadlines))

(define-actor (pool make-resource destroy-resource max-size idle-ttl)
  #:state (state
           #;stopped? #f
           #;total 0
           #;idle null
           #;busy null
           #;waiters null
           #;deadline (make-hasheq))
  #:event (lambda (st)
            (match-define (state _ total _ busy waiters deadlines) st)
            (cond
              [(hash-empty? deadlines) never-evt]
              [else
               (handle-evt
                (alarm-evt
                 (+ (apply min (hash-values deadlines))
                    (current-idle-timeout-slack))
                 #;monotonic? #t)
                (lambda (_)
                  (define t (current-inexact-monotonic-milliseconds))
                  (define-values (next-idle n-destroyed)
                    (for/fold ([next-idle null]
                               [n-destroyed 0])
                              ([(res deadline) (in-hash deadlines)])
                      (cond
                        [(< deadline t)
                         (destroy-resource res)
                         (hash-remove! deadlines res)
                         (values next-idle (add1 n-destroyed))]
                        [else
                         (values (cons res next-idle) n-destroyed)])))
                  (log-resource-pool-debug "destroyed ~s idle resource(s)" n-destroyed)
                  (state
                   #;stopped? #f
                   #;total (- total n-destroyed)
                   #;idle next-idle
                   #;busy busy
                   #;waiters waiters
                   #;deadlines deadlines)))]))
  #:stopped? state-stopped?

  (define (abandon st waiter)
    (match-define (state _ total idle busy waiters deadlines) st)
    (values
     (state
      #;stopped #f
      #;total total
      #;idle idle
      #;busy busy
      #;waiters (remq waiter waiters)
      #;deadlines deadlines)
     (void)))

  (define (lease st maybe-waiter)
    (match-define (state _ total idle busy waiters deadlines) st)
    (cond
      [(not (null? idle))
       (define res (car idle))
       (hash-remove! deadlines res)
       (log-resource-pool-debug "leased ~.s" res)
       (values
        (state
         #;stopped? #f
         #;total total
         #;idle (cdr idle)
         #;busy (cons res busy)
         #;waiters (remq maybe-waiter waiters)
         #;deadlines deadlines)
        `(ok ,res))]
      [(< total max-size)
       (define res (make-resource))
       (log-resource-pool-debug "created ~.s" res)
       (values
        (state
         #;stopped? #f
         #;total (add1 total)
         #;idle idle
         #;busy (cons res busy)
         #;waiters (remq maybe-waiter waiters)
         #;deadlines deadlines)
        `(ok ,res))]
      [else
       (define waiter (make-semaphore))
       (values
        (state
         #;stopped #f
         #;total total
         #;idle idle
         #;busy busy
         #;waiters (cons waiter (remq maybe-waiter waiters))
         #;deadlines deadlines)
        `(wait ,waiter))]))

  (define (release st res)
    (match-define (state _ total idle busy waiters deadlines) st)
    (cond
      [(memq res busy)
       (hash-set! deadlines res (+ (current-inexact-monotonic-milliseconds) idle-ttl))
       (for-each semaphore-post waiters)
       (log-resource-pool-debug "released ~.s" res)
       (values
        (state
         #;stopped? #f
         #;total total
         #;idle (cons res idle)
         #;busy (remq res busy)
         #;waiters waiters
         #;deadlines deadlines)
        (void))]
      [else
       (oops "released resource was never leased: ~.s" res)]))

  (define (close st)
    (match-define (state _ _ idle busy _ _) st)
    (cond
      [(null? busy)
       (log-resource-pool-debug "destroying ~s idle resource(s)" (length idle))
       (for-each destroy-resource idle)
       (values
        (state
         #;stopped? #t
         #;total 0
         #;idle null
         #;busy null
         #;waiters null
         #;deadlines (make-hasheq))
        (void))]
      [else
       (oops "attempted to close pool without releasing all the resources")])))

(define (oops msg . args)
  (raise
   (exn:fail:pool
    (apply format msg args)
    (current-continuation-marks))))
