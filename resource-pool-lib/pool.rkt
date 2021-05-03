#lang racket/base

(require racket/contract
         racket/match)

(provide
 exn:fail:pool?

 pool?
 make-pool
 pool-take!
 pool-release!
 pool-close!
 call-with-pool-resource

 current-idle-timeout-slack)

(define-logger resource-pool)

(struct exn:fail:pool exn:fail ())

(struct pool (req-ch mgr [closed? #:mutable]))

(struct req (nack-evt res-ch))
(struct lease-req req ())
(struct release-req req (r))
(struct close-req req ())

(define (oops msg . args)
  (exn:fail:pool
   (apply format msg args)
   (current-continuation-marks)))

(define current-idle-timeout-slack
  (make-parameter (* 15 1000)))

(define/contract (make-pool make-resource
                            [destroy-resource void]
                            #:max-size [max-size 8]
                            #:idle-ttl [idle-ttl (* 3600 1000)])
  (->* ((-> any/c))
       ((-> any/c void?)
        #:max-size exact-positive-integer?
        #:idle-ttl (or/c +inf.0 exact-positive-integer?))
       pool?)
  (define req-ch (make-channel))
  (define mgr (make-mgr req-ch make-resource destroy-resource max-size idle-ttl))
  (pool req-ch mgr #f))

(define (make-mgr req-ch make-resource destroy-resource max-size idle-ttl)
  (thread/suspend-to-kill
   (lambda ()
     (define slack (current-idle-timeout-slack))
     (define deadlines (make-hasheq))
     (define (reset-deadline! r)
       (hash-set! deadlines r (+ (current-inexact-milliseconds) idle-ttl)))
     (define (remove-deadline! r)
       (hash-remove! deadlines r))

     (let loop ([total 0]
                [idle null]
                [busy null]
                [requests null])
       (define idle-timeout-evt
         (if (hash-empty? deadlines)
             never-evt
             (alarm-evt (+ (apply min (hash-values deadlines)) slack))))

       (apply
        sync
        (handle-evt
         req-ch
         (match-lambda
           [`(lease ,nack-evt ,res-ch)
            (define req (lease-req nack-evt res-ch))
            (cond
              [(and (null? idle) (< total max-size))
               (define r (make-resource))
               (reset-deadline! r)
               (log-resource-pool-debug "created ~e" r)
               (loop (add1 total) (cons r idle) busy (cons req requests))]

              [else
               (loop total idle busy (cons req requests))])]

           [`(release ,r ,nack-evt ,res-ch)
            (define req (release-req nack-evt res-ch r))
            (loop total idle busy (cons req requests))]

           [`(close ,nack-evt ,res-ch)
            (define req (close-req nack-evt res-ch))
            (loop total idle busy (cons req requests))]))

        (handle-evt
         idle-timeout-evt
         (lambda (_)
           (define now (current-inexact-milliseconds))
           (define-values (idle* n-destroyed)
             (for/fold ([idle* null]
                        [n-destroyed 0])
                       ([(r deadline) (in-hash deadlines)])
               (cond
                 [(< deadline now)
                  (remove-deadline! r)
                  (with-handlers ([exn:fail?
                                   (lambda (e)
                                     (log-resource-pool-warning "failed to destroy idle resource: ~a~n  resource: ~e" (exn-message e) r))])
                    (destroy-resource r))
                  (values idle* (add1 n-destroyed))]
                 [else
                  (values (cons r idle*) n-destroyed)])))
           (log-resource-pool-debug "destroyed ~s idle resource(s)" n-destroyed)
           (loop (- total n-destroyed) idle* busy requests)))

        (append
         (for/list ([req (in-list requests)])
           (match req
             [(lease-req _ res-ch)
              (cond
                [(null? idle) never-evt]
                [else
                 (define r (car idle))
                 (handle-evt
                  (channel-put-evt res-ch r)
                  (lambda (_)
                    (remove-deadline! r)
                    (log-resource-pool-debug "leased ~e" r)
                    (loop total (cdr idle) (cons r busy) (remq req requests))))])]

             [(release-req _ res-ch r)
              (cond
                [(memq r busy)
                 (handle-evt
                  (channel-put-evt res-ch 'released)
                  (lambda (_)
                    (reset-deadline! r)
                    (log-resource-pool-debug "released ~e" r)
                    (loop total (cons r idle) (remq r busy) (remq req requests))))]

                [else
                 (handle-evt
                  (channel-put-evt res-ch (oops "released resource was never leased: ~e" r))
                  (lambda (_)
                    (loop total idle busy (remq req requests))))])]

             [(close-req _ res-ch)
              (cond
                [(null? busy)
                 (handle-evt
                  (channel-put-evt res-ch 'closed)
                  (lambda (_)
                    (log-resource-pool-debug "destroying all idle resources")
                    (for-each destroy-resource idle)
                    (log-resource-pool-debug "stopping resource pool manager")))]

                [else
                 (handle-evt
                  (channel-put-evt res-ch (oops "attempted to close pool without releasing all the resources"))
                  (lambda (_)
                    (loop total idle busy (remq req requests))))])]))
         (for/list ([req (in-list requests)])
           (handle-evt
            (req-nack-evt req)
            (lambda (_)
              (loop total idle busy (remq req requests)))))))))))

(define/contract (call-with-pool-resource p f #:timeout [timeout #f])
  (->* (pool? (-> any/c any))
       (#:timeout (or/c #f exact-nonnegative-integer?))
       any)
  (define r #f)
  (dynamic-wind
    (lambda ()
      (set! r (pool-take! p timeout))
      (unless r
        (raise (oops "timed out while taking resource"))))
    (lambda ()
      (f r))
    (lambda ()
      (pool-release! p r))))

(define/contract (pool-take! p [timeout #f])
  (->* (pool?) ((or/c #f exact-nonnegative-integer?)) (or/c #f any/c))
  (sync
   (pool-evt p 'lease)
   (if timeout
       (handle-evt
        (alarm-evt (+ (current-inexact-milliseconds) timeout))
        (lambda (_) #f))
       never-evt)))

(define/contract (pool-release! p r)
  (-> pool? any/c void?)
  (sync
   (wrap-evt
    (pool-evt p 'release r)
    (lambda (e)
      (when (exn:fail? e)
        (raise e))))))

(define/contract (pool-close! p)
  (-> pool? void?)
  (sync
   (wrap-evt
    (pool-evt p 'close)
    (lambda (e)
      (when (exn:fail? e)
        (raise e))
      (set-pool-closed?! p #t)))))

(define (pool-evt p id . args)
  (when (pool-closed? p)
    (raise (oops "pool closed")))
  (nack-guard-evt
   (lambda (nack-evt)
     (define res-ch (make-channel))
     (begin0 res-ch
       (thread-resume (pool-mgr p) (current-thread))
       (channel-put (pool-req-ch p) `(,id ,@args ,nack-evt ,res-ch))))))
