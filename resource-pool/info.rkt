#lang info

(define version "0.0")
(define collection 'multi)
(define deps '("base"
               "resource-pool-lib"))
(define build-deps '("rackcheck"
                     "racket-doc"
                     "rackunit-lib"
                     "resource-pool-lib"
                     "scribble-lib"))
(define implies '("resource-pool-lib"))
(define update-implies '("resource-pool-lib"))
