#lang racket/base

(require actor
         racket/match)

(provide
 exn:fail:pool?
 current-idle-timeout-slack
 pool lease-evt release close oops)

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
            (match-define (state _ total idle busy waiters deadlines) st)
            (define idle-deadline-evt
              (if (hash-empty? deadlines)
                  never-evt
                  (handle-evt
                   (alarm-evt
                    (+ (apply min (hash-values deadlines))
                       (current-idle-timeout-slack))
                    #;monotonic? #t)
                   (lambda (_)
                     (define t (current-inexact-monotonic-milliseconds))
                     (define-values (remaining-idle n-destroyed)
                       (for/fold ([remaining-idle null]
                                  [n-destroyed 0])
                                 ([(res deadline) (in-hash deadlines)])
                         (cond
                           [(< deadline t)
                            (destroy-resource res)
                            (hash-remove! deadlines res)
                            (values remaining-idle (add1 n-destroyed))]
                           [else
                            (values (cons res remaining-idle) n-destroyed)])))
                     (log-resource-pool-debug "expired ~s idle resource(s)" n-destroyed)
                     (state
                      #;stopped? #f
                      #;total (- total n-destroyed)
                      #;idle remaining-idle
                      #;busy busy
                      #;waiters waiters
                      #;deadlines deadlines)))))
            (define waiter-res-evts
              (if (null? idle)
                  (list)
                  (for/list ([waiter (in-list waiters)])
                    (match-define (cons res-ch _) waiter)
                    (match-define (cons res remaining-idle) idle)
                    (handle-evt
                     (channel-put-evt res-ch res)
                     (lambda (_)
                       (hash-remove! deadlines res)
                       (log-resource-pool-debug "leased ~.s" res)
                       (struct-copy
                        state st
                        [idle remaining-idle]
                        [busy (cons res busy)]
                        [waiters (remq waiter waiters)]))))))
            (define waiter-nack-evts
              (for/list ([waiter (in-list waiters)])
                (match-define (cons _ nack-evt) waiter)
                (handle-evt
                 nack-evt
                 (lambda (_)
                   (struct-copy
                    state st
                    [waiters (remq waiter waiters)])))))
            (apply
             choice-evt
             idle-deadline-evt
             (append
              waiter-res-evts
              waiter-nack-evts)))
  #:stopped? state-stopped?

  (define (lease st res-ch nack-evt)
    (match-define (state _ total idle busy waiters deadlines) st)
    (define waiter (cons res-ch nack-evt))
    (cond
      [(and
        (null? idle)
        (< total max-size))
       (define res (make-resource))
       (hash-set! deadlines res (+ (current-inexact-monotonic-milliseconds) idle-ttl))
       (log-resource-pool-debug "created ~.s" res)
       (values
        (state
         #;stopped? #f
         #;total (add1 total)
         #;idle (cons res idle)
         #;busy busy
         #;waiters (cons waiter waiters)
         #;deadlines deadlines)
        (void))]
      [else
       (values
        (state
         #;stopped #f
         #;total total
         #;idle idle
         #;busy busy
         #;waiters (cons waiter waiters)
         #;deadlines deadlines)
        (void))]))

  (define (release st res)
    (match-define (state _ total idle busy waiters deadlines) st)
    (unless (memq res busy)
      (oops "released resource was never leased: ~.s" res))
    (hash-set! deadlines res (+ (current-inexact-monotonic-milliseconds) idle-ttl))
    (log-resource-pool-debug "released ~.s" res)
    (values
     (state
      #;stopped? #f
      #;total total
      #;idle (cons res idle)
      #;busy (remq res busy)
      #;waiters waiters
      #;deadlines deadlines)
     (void)))

  (define (close st)
    (match-define (state _ _ idle busy _ _) st)
    (unless (null? busy)
      (oops "attempted to close pool without releasing all the resources"))
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
     (void))))

(define (oops msg . args)
  (raise
   (exn:fail:pool
    (apply format msg args)
    (current-continuation-marks))))
