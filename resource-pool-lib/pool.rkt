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
 call-with-pool-resource)

(define-logger resource-pool)

(struct exn:fail:pool exn:fail ())

(struct pool (custodian mgr))

(define/contract (make-pool make-resource
                            [destroy-resource void]
                            #:max-size [max-size 8]
                            #:idle-ttl [idle-ttl (* 3600 1000)])
  (->* ((-> any/c))
       ((-> any/c void?)
        #:max-size exact-positive-integer?
        #:idle-ttl (or/c +inf.0 exact-positive-integer?))
       pool?)
  (define custodian (make-custodian))
  (parameterize ([current-custodian custodian])
    (pool custodian (make-mgr make-resource destroy-resource max-size idle-ttl))))

(define (make-mgr make-resource destroy-resource max-size idle-ttl)
  (thread
   (lambda ()
     (define deadlines (make-hasheq))
     (define (reset-deadline! r)
       (hash-set! deadlines r (+ (current-inexact-milliseconds) idle-ttl)))
     (define (remove-deadline! r)
       (hash-remove! deadlines r))

     (let loop ([total 0]
                [idle null]
                [busy null]
                [waiters null])
       (define idle-timeout-evt
         (if (hash-empty? deadlines)
             never-evt
             (alarm-evt (+ (apply min (hash-values deadlines)) (* 60 1000)))))

       (sync
        (handle-evt
         (thread-receive-evt)
         (lambda (_)
           (match (thread-receive)
             [`(lease ,ch)
              (cond
                [(not (null? idle))
                 (define r (car idle))
                 (cond
                   [(channel-try-put ch r)
                    (remove-deadline! r)
                    (log-resource-pool-debug "leased idle resource ~e" r)
                    (loop total (cdr idle) (cons r busy) waiters)]

                   [else
                    (log-resource-pool-debug "idle resource ~e could not be leased" r)
                    (loop total idle busy waiters)])]

                [(< total max-size)
                 (define r (make-resource))
                 (cond
                   [(channel-try-put ch r)
                    (log-resource-pool-debug "leased new resource ~e" r)
                    (loop (add1 total) idle (cons r busy) waiters)]

                   [else
                    (log-resource-pool-debug "new resource ~e could not be leased" r)
                    (loop (add1 total) (cons r idle) busy waiters)])]

                [else
                 (log-resource-pool-debug "adding channel to waitlist")
                 (loop total idle busy (append waiters (list ch)))])]

             [`(release ,r ,ch)
              (cond
                [(memq r busy)
                 (let waiter-loop ([waiters waiters])
                   (cond
                     [(null? waiters)
                      (log-resource-pool-debug "adding ~e to idle set" r)
                      (reset-deadline! r)
                      (channel-try-put ch 'released)
                      (loop total (cons r idle) (remq r busy) waiters)]

                     [else
                      (define waiter-ch (car waiters))
                      (cond
                        [(channel-try-put waiter-ch r)
                         (channel-try-put ch 'released)
                         (log-resource-pool-debug "sent ~e to head waiter" r)
                         (loop total idle busy (cdr waiters))]

                        [else
                         (waiter-loop (cdr waiters))])]))]

                [else
                 (log-resource-pool-warning "resource ~e was never leased" r)
                 (channel-try-put ch (exn:fail:pool
                                      (format "released resource was never leased: ~e" r)
                                      (current-continuation-marks)))
                 (loop total idle busy waiters)])]

             [`(close ,ch)
              (cond
                [(null? busy)
                 (log-resource-pool-debug "destroying all idle resources")
                 (with-handlers ([exn:fail?
                                  (lambda (e)
                                    (channel-try-put ch e)
                                    (loop total idle busy waiters))])
                   (for-each destroy-resource idle)
                   (channel-try-put ch 'closed)
                   (log-resource-pool-debug "closing resource pool manager"))]

                [else
                 (log-resource-pool-warning "attempted to close pool without releasing all resources")
                 (channel-try-put ch (exn:fail:pool
                                      "attempted to close pool without releasing all the resources"
                                      (current-continuation-marks)))
                 (loop total idle busy waiters)])])))

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
           (loop (- total n-destroyed) idle* busy waiters))))))))

(define-syntax-rule (dispatch p id arg ...)
  (thread-send (pool-mgr p) (list 'id arg ...)))

(define-syntax-rule (dispatch/blocking p id arg ...)
  (let ([ch (make-channel)])
    (dispatch p id arg ... ch)
    (define maybe-exn (sync ch))
    (begin0 maybe-exn
      (when (exn? maybe-exn)
        (raise maybe-exn)))))

(define/contract (pool-take! p [timeout #f])
  (->* (pool?) ((or/c #f exact-nonnegative-integer?)) (or/c #f any/c))
  (define ch (make-channel))
  (dispatch p lease ch)
  (sync
   ch
   (if timeout
       (handle-evt
        (alarm-evt (+ (current-inexact-milliseconds) timeout))
        (lambda (_) #f))
       never-evt)))

(define/contract (pool-release! p r)
  (-> pool? any/c void?)
  (void (dispatch/blocking p release r)))

(define/contract (pool-close! p)
  (-> pool? void?)
  (void (dispatch/blocking p close)))

(define/contract (call-with-pool-resource p f #:timeout [timeout #f])
  (->* (pool? (-> any/c any))
       (#:timeout (or/c #f exact-nonnegative-integer?))
       any)
  (define r #f)
  (dynamic-wind
    (lambda ()
      (set! r (pool-take! p timeout))
      (unless r
        (raise (exn:fail:pool
                "timed out while taking resource"
                (current-continuation-marks)))))
    (lambda ()
      (f r))
    (lambda ()
      (pool-release! p r))))

(define (channel-try-put ch v)
  (sync/timeout 0 (channel-put-evt ch v)))
