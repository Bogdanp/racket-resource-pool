#lang racket/base

(module+ test
  (require data/pool
           rackcheck
           racket/match
           rackunit
           rackunit/text-ui)

  (define gen:timeout
    (gen:integer-in 100 500))

  (define gen:command
    (gen:frequency
     `((5 . ,(gen:const `(take)))
       (5 . ,(gen:let ([timeout gen:timeout])
               (gen:const `(take/timeout ,timeout))))
       (5 . ,(gen:const `(release)))
       (5 . ,(gen:let ([timeout gen:timeout])
               (gen:const `(release-after ,timeout))))
       (1 . ,(gen:const `(close))))))

  (define gen:commands
    (gen:let ([commands (gen:list gen:command)])
      (append commands '((close)))))

  (struct state (p size stack thds pending closed?)
    #:transparent)

  (define (box-update b f)
    (let loop ([v (unbox b)])
      (define new-v (f v))
      (cond
        [(box-cas! b v (f v)) new-v]
        [else (loop (unbox b))])))

  (define-syntax-rule (thd e0 e ...)
    (let ([ch (make-channel)])
      (begin0 ch
        (thread
         (lambda ()
           (with-handlers ([exn:fail? (λ (ex)
                                        (channel-put ch ex))])
             e0 e ...
             (channel-put ch 'done)))))))

  (define/match (interp s c)
    [((state p size stack _ pending closed?)
      '(take))
     (cond
       [closed?
        (begin0 s
          (check-exn
           #rx"target thread is not running"
           (lambda ()
             (pool-release! p (gensym)))))]

       [else
        (define taken
          (+ (length stack)
             (length (unbox pending))))
        (define r (pool-take! p 100))
        (when (< taken size)
          (check-not-false r))
        (struct-copy state s [stack (if r (cons r stack) stack)])])]

    [((state p _ _ thds pending closed?)
      `(take/timeout ,timeout))
     (cond
       [closed?
        (struct-copy state s
                     [thds (cons
                            (thd
                             (check-exn
                              #rx"target thread is not running"
                              (lambda ()
                                (pool-take! p timeout))))
                            thds)])]

       [else
        (struct-copy state s
                     [thds (cons
                            (thd
                             (define r (pool-take! p timeout))
                             (when r
                               (sync (system-idle-evt))
                               (check-false (memq r (unbox pending)))
                               (pool-release! p r)))
                            thds)])])]

    [((state p _ stack _ _ closed?)
      '(release))
     (cond
       [(null? stack)
        (begin0 s
          (check-exn
           (if closed?
               #rx"target thread is not running"
               #rx"never leased")
           (lambda ()
             (pool-release! p (gensym)))))]

       [else
        (pool-release! p (car stack))
        (struct-copy state s [stack (cdr stack)])])]

    [((state p _ stack thds pending closed?)
      `(release-after ,timeout))
     (cond
       [(null? stack)
        (struct-copy state s [thds (cons
                                    (thd
                                     (check-exn
                                      (if closed?
                                          #rx"target thread is not running"
                                          #rx"never leased")
                                      (lambda ()
                                        (sleep (/ timeout 1000.0))
                                        (pool-release! p (gensym)))))
                                    thds)])]

       [else
        (define v (car stack))
        (box-update pending (λ (vs)
                              (cons v vs)))
        (struct-copy state s
                     [stack (cdr stack)]
                     [thds (cons
                            (thd
                             (sleep (/ timeout 1000.0))
                             (pool-release! p v)
                             (box-update pending (λ (vs)
                                                   (remq v vs))))
                            thds)])])]

    [((state p _ stack thds pending closed?)
      '(close))
     (for ([ch (in-list thds)])
       (define maybe-exn (sync ch))
       (when (exn? maybe-exn)
         (raise maybe-exn)))

     (cond
       [closed?
        (begin0 (struct-copy state s [thds null])
          (check-exn
           #rx"target thread is not running"
           (lambda ()
             (pool-close! p))))]

       [(and (null? stack)
             (null? (unbox pending)))
        (begin0 (struct-copy state s
                             [thds null]
                             [closed? #t])
          (pool-close! p))]

       [else
        (begin0 (struct-copy state s [thds null])
          (check-exn
           #rx"attempted to close pool without releasing all the resources"
           (lambda ()
             (pool-close! p))))])])

  (define (interp* size cs)
    (define p
      (make-pool
       #:max-size size
       (let ([seq (box 0)])
         (lambda ()
           (box-update seq add1)))))
    (for/fold ([s (state p size null null (box null) #f)])
              ([c (in-list cs)])
      (interp s c)))

  (run-tests
   (test-suite
    "data/pool"

    (test-case "sync access"
      (define-property prop:sync
        ([size (gen:integer-in 1 8)]
         [commands gen:commands])
        (interp* size commands))

      (check-property
       (make-config
        #:tests 50
        #:deadline (+ (current-inexact-milliseconds) (* 600 1000)))
       prop:sync)))))
