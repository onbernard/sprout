# SPROUT
Another stupid library named after a vegetable.

## Capabilities
1. Asynchronous task scheduling with free caching
2. Infinite tasks

## Caveats
1. For a task defined by a generator, yielding in order to signal progress might make it give up scheduling to another task. It may cause problem with shared ressources
2. Return values are stored in redis. Mind the big ones

## TODOs
- Check correct serialization of return values
- Add progress callback without generators
- Synchronization primitives
- Let the user choose wich arguments to select for hashing
- Task defined by asynchronous functions, generators
- Context global variable for dependency injection
- Task signature serialization > export outside of definition
- Javascript w/ SSE
- Monitoring & CLI
- In memory, on disk memory
- Add proper tests
- Add generic typing
- Better logging
- Communication with generator tasks ? i.e. via yield
- Make a then function
- Set failure protocols
- Add a watch function to task that returns a monitor