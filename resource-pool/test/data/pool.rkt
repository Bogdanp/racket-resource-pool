#lang racket/base

(module+ test
  (require data/pool
           rackcheck
           racket/match
           racket/promise
           rackunit
           rackunit/text-ui)

  (define gen:command
    (gen:frequency
     `((5 . ,(gen:const `(take)))
       (5 . ,(gen:const `(release)))
       (1 . ,(gen:const `(expire)))
       (1 . ,(gen:const `(close))))))

  (define gen:commands
    (gen:let ([commands (gen:list gen:command)])
      (append commands '((close)))))

  (struct state
    (p           ;; pool
     size        ;; pool size
     ttl         ;; item ttl
     stack       ;; stack-set of leased items
     expired     ;; set of expired items
     closed?     ;; has the pool been successfully closed?
     )
    #:transparent)

  (define (make-state p size ttl)
    (state p size ttl null null #f))

  (define (box-update b f)
    (let loop ([v (unbox b)])
      (define new-v (f v))
      (cond
        [(box-cas! b v (f v)) new-v]
        [else (loop (unbox b))])))

  (define/match (interp _c s)
    [('(take) (state p _size _ttl _stack _expired #t))
     (begin0 s
       (check-exn
        #rx"stopped"
        (lambda ()
          (pool-release! p (gensym)))))]

    [('(take) (state p size _ttl stack expired #f))
     (define taken (length stack))
     (define r (pool-take! p 100))
     (when (< taken size)
       (check-not-false r))
     (when r
       (check-false (memq r expired)))
     (struct-copy state s [stack (if r (cons r stack) stack)])]

    [('(release) (state p _size _ttl '() _expired closed?))
     (begin0 s
       (check-exn
        (if closed?
            #rx"stopped"
            #rx"never leased")
        (lambda ()
          (pool-release! p (gensym)))))]

    [('(release) (state p _size _ttl stack _expired _closed?))
     (pool-release! p (car stack))
     (struct-copy state s [stack (cdr stack)])]

    [('(expire) (state p _size _ttl '() _expired closed?))
     (begin0 s
       (check-exn
        (if closed?
            #rx"stopped"
            #rx"never leased")
        (lambda ()
          (pool-release! p (gensym)))))]

    [('(expire) (state p _size ttl stack expired _closed?))
     (define v (car stack))
     (pool-release! p v)
     (sync (alarm-evt (+ (current-inexact-monotonic-milliseconds) (* 2 ttl)) #t))
     (sync (system-idle-evt))
     (struct-copy state s
                  [stack (cdr stack)]
                  [expired (cons v expired)])]

    [('(close) (state p _size _ttl _stack _expired #t))
     (begin0 s
       (check-exn
        #rx"stopped"
        (lambda ()
          (pool-close! p))))]

    [('(close) (state p _size _ttl '() _expired #f))
     (begin0 (struct-copy state s [closed? #t])
       (pool-close! p))]

    [('(close) (state p _size _ttl _stack _expired #f))
     (begin0 s
       (check-exn
        #rx"attempted to close pool without releasing all the resources"
        (lambda ()
          (pool-close! p))))])

  (define (interp* size ttl cs)
    (define p
      (make-pool
       #:max-size size
       #:idle-ttl ttl
       (let ([seq (box 0)])
         (lambda ()
           (box-update seq add1)))))
    (for/fold ([s (make-state p size ttl)])
              ([c (in-list cs)])
      (interp c s)))

  (parameterize ([current-idle-timeout-slack 0])
    (run-tests
     (test-suite
      "data/pool"

      (test-case "basic use"
        (define seq 0)
        (define p
          (make-pool
           (lambda ()
             (begin0 seq
               (set! seq (add1 seq))))))
        (define v0 (pool-take! p))
        (check-equal? v0 0)
        (define v1 (pool-take! p))
        (check-equal? v1 1)
        (pool-release! p v0)
        (define v2 (pool-take! p))
        (check-equal? v2 v0)
        (pool-release! p v1)
        (check-exn
         #rx"attempted to close pool without releasing all the resources"
         (λ () (pool-close! p)))
        (pool-release! p v2)
        (pool-close! p)
        (check-exn
         #rx"stopped"
         (λ () (pool-close! p))))

      (test-case "provisioning"
        (define p (make-pool #:max-size 2 gensym))
        (define v0 (pool-take! p))
        (check-not-false v0)
        (define v1 (pool-take! p 100))
        (check-not-false v1)
        (pool-release! p v0)
        (pool-release! p v1)
        (pool-close! p))

      (test-case "kill safety"
        (define sema
          (make-semaphore))
        (define p
          (make-pool
           (lambda ()
             (semaphore-wait sema)
             (gensym))))
        (define thd
          (thread
           (lambda ()
             (pool-take! p))))
        (sync (system-idle-evt))
        (kill-thread thd)
        (semaphore-post sema)
        (check-not-false (pool-take! p)))

      (test-case "take evts"
        (define sema
          (make-semaphore))
        (define p
          (make-pool
           (lambda ()
             (semaphore-wait sema)
             (gensym))))
        (define e
          (pool-take!-evt p))
        (semaphore-post sema)
        (define res (sync e))
        (check-not-false res)
        (check-false (sync/timeout 0.01 e))
        (pool-release! p res)
        (define res2 (sync/timeout 0.01 e))
        (check-eq? res res2)
        (pool-release! p res))

      (test-case "make-resource can return an exn"
        (define p
          (make-pool
           (lambda ()
             (exn:fail "fail" (current-continuation-marks)))))
        (define e (pool-take! p))
        (check-not-false e)
        (pool-release! p e)
        (pool-close! p))

      (test-case "make-resource can raise an exn"
        (define fail? #f)
        (define p
          (make-pool
           (lambda ()
             (if fail?
                 (error 'fail)
                 (gensym)))))
        (define v (pool-take! p))
        (check-not-false v)
        (set! fail? #t)
        (check-exn
         #rx"fail"
         (lambda ()
           (pool-take! p)))
        (pool-release! p v)
        (pool-close! p))

      (test-case "make-resource raising an exn doesn't oversubscribe pool"
        (define fail? #t)
        (define p
          (make-pool
           #:max-size 1
           (lambda ()
             (if fail?
                 (error 'fail)
                 (gensym)))))
        (check-exn
         #rx"fail"
         (lambda ()
           (pool-take! p)))
        (set! fail? #f)
        (define e (pool-take! p 100))
        (check-not-false e)
        (pool-release! p e)
        (pool-close! p))

      (test-case "make-resource raising an exn doesn't oversubscribe pool 2"
        (define p
          (make-pool
           #:max-size 1
           (lambda ()
             (error 'fail))))
        (define t1 (delay/thread (pool-take! p)))
        (define t2 (delay/thread (pool-take! p)))
        (check-exn #rx"fail" (lambda () (force t1)))
        (check-exn #rx"fail" (lambda () (force t2)))
        (pool-close! p))

      (test-case "sync access"
        (define-property prop:sync
          ([size (gen:integer-in 1 8)]
           [idle-ttl (gen:integer-in 50 100)]
           [commands gen:commands])
          (interp* size idle-ttl commands))

        (check-property
         (make-config
          #:tests (if (getenv "CI") 50 150)
          #:deadline (+ (current-inexact-milliseconds) (* 30 60 1000)))
         prop:sync))))))
