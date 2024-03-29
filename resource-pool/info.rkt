#lang info

(define license 'BSD-3-Clause)
(define version "0.1")
(define collection 'multi)
(define deps '("base"
               "resource-pool-lib"))
(define build-deps '("rackcheck-lib"
                     "racket-doc"
                     "rackunit-lib"
                     "resource-pool-lib"
                     "scribble-lib"))
(define implies '("resource-pool-lib"))
