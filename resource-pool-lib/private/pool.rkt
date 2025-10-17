#lang racket/base

(require actor
         racket/match
         racket/promise)

(provide
 exn:fail:pool?
 current-idle-timeout-slack
 pool lease-evt release abandon close oops
 (struct-out err)
 (struct-out ok))

(define-logger resource-pool)

(define current-idle-timeout-slack
  (make-parameter (* 15 1000)))

(struct exn:fail:pool exn:fail ())
(struct state (stopped? total idle busy exns waiters promises deadlines))

(struct err (e))
(struct ok (v))

;; total = #idle + #busy + #exns + #promises
(define-actor (pool make-resource destroy-resource max-size idle-ttl)
  #:state (state
           #;stopped? #f
           #;total 0
           #;idle null
           #;busy null
           #;exns null
           #;waiters null
           #;promises null
           #;deadlines (hasheq))
  #:event (let ([slack (current-idle-timeout-slack)])
            (lambda (st)
              (match-define (state _ total idle busy exns waiters promises deadlines) st)
              (define wanted (min max-size (length waiters)))
              (define needed (max 0 (- wanted total)))
              (define make-resources-evt
                (if (positive? needed)
                    (handle-evt
                     always-evt
                     (lambda (_)
                       (define the-promises
                         (for/list ([_ (in-range needed)])
                           (delay/thread (make-resource))))
                       (struct-copy
                        state st
                        [total (+ total needed)]
                        [promises (append the-promises promises)])))
                    never-evt))
              (define idle-deadline-evt
                (if (hash-empty? deadlines)
                    never-evt
                    (handle-evt
                     (alarm-evt
                      #;msecs (+ (apply min (hash-values deadlines)) slack)
                      #;monotonic? #t)
                     (lambda (_)
                       (define t (current-inexact-monotonic-milliseconds))
                       (define-values (remaining-idle remaining-deadlines n-destroyed)
                         (for/fold ([remaining-idle null]
                                    [remaining-deadlines deadlines]
                                    [n-destroyed 0])
                                   ([(res deadline) (in-hash deadlines)])
                           (cond
                             [(< deadline t)
                              (destroy-resource res)
                              (values
                               remaining-idle
                               (hash-remove remaining-deadlines res)
                               (add1 n-destroyed))]
                             [else
                              (values
                               (cons res remaining-idle)
                               remaining-deadlines
                               n-destroyed)])))
                       (log-resource-pool-debug "expired ~s idle resource(s)" n-destroyed)
                       (state
                        #;stopped? #f
                        #;total (- total n-destroyed)
                        #;idle remaining-idle
                        #;busy busy
                        #;exns exns
                        #;waiters waiters
                        #;promises promises
                        #;deadlines remaining-deadlines)))))
              (define promise-evts
                (for/list ([promise (in-list promises)])
                  (handle-evt
                   promise
                   (lambda (_)
                     ;; XXX: Avoid decrementing total until the exn is handed off to a waiter. That
                     ;; way, new resources cannot be created when the pool is full until exns are
                     ;; handed off, thus having better backoff.
                     (with-handlers
                       ([exn:fail?
                         (lambda (e)
                           (struct-copy
                            state st
                            [exns (cons e exns)]
                            [promises (remq promise promises)]))])
                       (define res (force promise))
                       (log-resource-pool-debug "created ~a" (~res res))
                       (struct-copy
                        state st
                        [idle (cons res idle)]
                        [promises (remq promise promises)]
                        [deadlines (hash-set deadlines res (deadline idle-ttl))]))))))
              (define waiter-exn-evts
                (if (null? exns)
                    (list)
                    (for/list ([waiter (in-list waiters)])
                      (match-define (cons res-ch _) waiter)
                      (match-define (cons e remaining-exns) exns)
                      (handle-evt
                       (channel-put-evt res-ch (err e))
                       (lambda (_)
                         (log-resource-pool-debug "leased exn ~.s" e)
                         (struct-copy
                          state st
                          [total (sub1 total)]
                          [exns remaining-exns]
                          [waiters (remq waiter waiters)]))))))
              (define waiter-res-evts
                (if (null? idle)
                    (list)
                    (for/list ([waiter (in-list waiters)])
                      (match-define (cons res-ch _) waiter)
                      (match-define (cons res remaining-idle) idle)
                      (handle-evt
                       (channel-put-evt res-ch (ok res))
                       (lambda (_)
                         (log-resource-pool-debug "leased ~a" (~res res))
                         (struct-copy
                          state st
                          [idle remaining-idle]
                          [busy (cons res busy)]
                          [deadlines (hash-remove deadlines res)]
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
               make-resources-evt
               idle-deadline-evt
               (append
                promise-evts
                waiter-exn-evts
                waiter-res-evts
                waiter-nack-evts))))
  #:stopped? state-stopped?

  (define (lease st res-ch nack-evt)
    (match-define (state _ total idle busy exns waiters promises deadlines) st)
    (define waiter (cons res-ch nack-evt))
    (values
     (state
      #;stopped? #f
      #;total total
      #;idle idle
      #;busy busy
      #;exns exns
      #;waiters (cons waiter waiters)
      #;promises promises
      #;deadlines deadlines)
     (void)))

  (define (release st res)
    (match-define (state _ total idle busy exns waiters promises deadlines) st)
    (unless (memq res busy)
      (oops "released resource was never leased: ~a" (~res res)))
    (log-resource-pool-debug "released ~a" (~res res))
    (values
     (state
      #;stopped? #f
      #;total total
      #;idle (cons res idle)
      #;busy (remq res busy)
      #;exns exns
      #;waiters waiters
      #;promises promises
      #;deadlines (hash-set deadlines res (deadline idle-ttl)))
     (void)))

  (define (abandon st res)
    (match-define (state _ total idle busy exns waiters promises deadlines) st)
    (unless (memq res busy)
      (oops "abandoned resource was never leased: ~a" (~res res)))
    (destroy-resource res)
    (log-resource-pool-debug "abandoned ~a" (~res res))
    (values
     (state
      #;stopped? #f
      #;total (sub1 total)
      #;idle idle
      #;busy (remq res busy)
      #;exns exns
      #;waiters waiters
      #;promises promises
      #;deadlines deadlines)
     (void)))

  (define (close st)
    (match-define (state _ _ idle busy _ _ promises _) st)
    (unless (and (null? busy)
                 (null? promises))
      (oops "attempted to close pool without releasing all the resources"))
    (log-resource-pool-debug "destroying ~s idle resource(s)" (length idle))
    (for-each destroy-resource idle)
    (values
     (state
      #;stopped? #t
      #;total 0
      #;idle null
      #;busy null
      #;exns null
      #;waiters null
      #;promises null
      #;deadlines (make-hasheq))
     (void))))

(define (oops msg . args)
  (raise
   (exn:fail:pool
    (apply format msg args)
    (current-continuation-marks))))

(define (deadline ttl)
  (+ (current-inexact-monotonic-milliseconds) ttl))

(define (~res res)
  (format "~.s (~s)" res (eq-hash-code res)))
