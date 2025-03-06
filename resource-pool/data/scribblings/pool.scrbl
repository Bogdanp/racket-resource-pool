#lang scribble/manual

@(require (for-label data/pool
                     racket/base
                     racket/contract))

@title{Resource Pool}
@author[(author+email "Bogdan Popa" "bogdan@defn.io")]
@defmodule[data/pool]

This module provides a generic blocking @deftech{resource pool}
implementation. Useful for managing things such as database and HTTP
connections. Resource pools are both thread-safe and kill-safe.

@deftogether[(
  @defproc[(pool? [v any/c]) boolean?]
  @defproc[(make-pool [make-resource (-> any/c)]
                      [destroy-resource (-> any/c void?) void]
                      [#:max-size max-size exact-positive-integer? 8]
                      [#:idle-ttl idle-ttl (or/c +inf.0 exact-positive-integer?) (* 3600 1000)]) pool?]
)]{

  The @racket[make-pool] function returns a new @tech{resource pool}
  that lazily creates new resources using @racket[make-resource]. The
  resulting pool can contain up to @racket[#:max-size] resources.

  The @racket[#:idle-ttl] argument controls how long a resource can
  remain idle before @racket[destroy-resource] is applied to it and it
  is removed from the pool.
}

@defproc[(call-with-pool-resource [p pool?]
                                  [proc (-> any/c any)]
                                  [#:timeout timeout (or/c #f exact-nonnegative-integer?)]) any/c]{

  Leases a resource from @racket[p] and applies @racket[proc] to it,
  returning the leased value back into the pool once @racket[proc]
  finishes executing.

  The @racket[#:timeout] behaves the same as in @racket[pool-take!],
  except that if the timeout is hit, an @racket[exn:fail:pool?] is
  raised and @racket[#f] is not executed.
}

@defproc[(pool-take! [p pool?]
                     [timeout (or/c #f exact-nonnegative-integer?) #f]) (or/c #f any/c)]{

  Waits for a resource to become available and then leases it from
  @racket[p]. If the @racket[timeout] argument is provided, the function
  will block for at most @racket[timeout] milliseconds before returning.
  On timeout, @racket[#f] is returned.
}

@defproc[(pool-release! [p pool?]
                        [v any/c]) void?]{

  Releases @racket[v] back into @racket[p]. If @racket[v] was not leased
  from @racket[p], then an @racket[exn:fail:pool?] error is raised.
}

@defproc[(pool-close! [p pool?]) void?]{
  Closes @racket[p]. If @racket[pool-close] is called before all
  of the leased resources have been returned to the pool, an
  @racket[exn:fail:pool?] error is raised and the pool remains open.

  Raises an exception if @racket[p] has already been closed.
}

@defproc[(exn:fail:pool? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a @tech{resource pool} error.
}
