#lang racket/base

(require racket/contract/base
         racket/match
         (only-in "private/pool.rkt"
                  exn:fail:pool?
                  current-idle-timeout-slack
                  oops)
         (prefix-in actor: "private/pool.rkt"))

(provide
 (rename-out
  [actor:stats? pool-stats?]
  [actor:stats-open pool-stats-open]
  [actor:stats-busy pool-stats-busy]
  [actor:stats-idle pool-stats-idle])
 (contract-out
  [exn:fail:pool?
   (-> any/c boolean?)]
  [make-pool
   (->* [(-> any/c)]
        [(-> any/c void?)
         #:max-size exact-positive-integer?
         #:idle-ttl (or/c +inf.0 exact-positive-integer?)]
        pool?)]
  [pool?
   (-> any/c boolean?)]
  [pool-take!
   (->* [pool?]
        [(or/c #f exact-nonnegative-integer?)]
        (or/c #f any/c))]
  [pool-take!-evt
   (-> pool? evt?)]
  [pool-release!
   (-> pool? any/c void?)]
  [pool-abandon!
   (-> pool? any/c void?)]
  [pool-close!
   (-> pool? void?)]
  [pool-stats
   (-> pool? actor:stats?)]
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
  (sync
   (pool-take!-evt p)
   (if timeout
       (handle-evt
        (alarm-evt
         (+ timeout (current-inexact-monotonic-milliseconds))
         #;monotonic? #t)
        (λ (_) #f))
       never-evt)))

(define (pool-take!-evt p)
  (match-define (pool impl) p)
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (define ch (make-channel))
      (replace-evt
       (actor:lease-evt impl ch nack)
       (λ (_) ch))))
   (match-lambda
     [(actor:err e) (raise e)]
     [(actor:ok v) v])))

(define (pool-release! p res)
  (actor:release (pool-impl p) res))

(define (pool-abandon! p res)
  (actor:abandon (pool-impl p) res))

(define (pool-close! p)
  (actor:close (pool-impl p)))

(define (pool-stats p)
  (actor:get-stats (pool-impl p)))
