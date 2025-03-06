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

(define (pool-take! p [timeout #f])
  (match-define (pool impl) p)
  (sync
   (nack-guard-evt
    (lambda (nack)
      (define res-ch (make-channel))
      (replace-evt
       (actor:lease-evt impl res-ch nack)
       (lambda (_) res-ch))))
   (if timeout
       (handle-evt
        (alarm-evt
         (+ timeout (current-inexact-monotonic-milliseconds))
         #;monotonic? #t)
        (λ (_) #f))
       never-evt)))

(define (pool-release! p res)
  (actor:release (pool-impl p) res))

(define (pool-close! p)
  (actor:close (pool-impl p)))
