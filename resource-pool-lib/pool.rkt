#lang racket/base

(require racket/contract/base
         racket/match
         (only-in "private/pool.rkt"
                  exn:fail:pool?
                  current-idle-timeout-slack
                  oops)
         (prefix-in actor: "private/pool.rkt"))

(provide
 (contract-out
  [exn:fail:pool? (-> any/c boolean?)]
  [make-pool
   (->* [(-> any/c)]
        [(-> any/c void?)
         #:max-size exact-positive-integer?
         #:idle-ttl (or/c +inf.0 exact-positive-integer?)]
        pool?)]
  [pool? (-> any/c boolean?)]
  [pool-take! (->* [pool?] [(or/c #f exact-nonnegative-integer?)] (or/c #f any/c))]
  [pool-release! (-> pool? any/c void?)]
  [pool-close! (-> pool? void?)]
  [call-with-pool-resource
    (->* [pool? (-> any/c any)]
         [#:timeout (or/c #f exact-nonnegative-integer?)]
         any)]
  [current-idle-timeout-slack (parameter/c real?)]))

(struct pool (impl))

(define (make-pool make-resource
                   [destroy-resource void]
                   #:max-size [max-size 8]
                   #:idle-ttl [idle-ttl (* 3600 1000)])
  (pool (actor:pool make-resource destroy-resource max-size idle-ttl)))

(define (call-with-pool-resource
          #:timeout [timeout #f]
          p proc)
  (define res #f)
  (dynamic-wind
    (lambda ()
      (set! res (pool-take! p timeout))
      (unless res
        (oops "timed out while taking resource")))
    (lambda ()
      (proc res))
    (lambda ()
      (pool-release! p res))))

(define (pool-take! p [t #f])
  (match-define (pool impl) p)
  (define deadline
    (and t (+ t (current-inexact-monotonic-milliseconds))))
  (let loop ([waiter #f]
             [waiting? #f])
    (cond
      [waiting?
       (sync
        (handle-evt
         (alarm-evt deadline t)
         (lambda (_)
           (begin0 #f
             (actor:abandon impl waiter))))
        (handle-evt
         waiter
         (lambda (_)
           (loop waiter #f))))]
      [else
       (match (actor:lease impl waiter)
         [`(ok ,res) res]
         [`(wait ,waiter) (loop waiter #t)])])))

(define (pool-release! p res)
  (actor:release (pool-impl p) res))

(define (pool-close! p)
  (actor:close (pool-impl p)))
