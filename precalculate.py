"""Precalculate expensive-to-create objects in a thread pool."""

from typing import Callable, TypeVar
from queue import Queue
from multiprocessing.pool import ThreadPool


__all__ = ["CreatedObject", "Precalculator"]


CreatedObject = TypeVar("CreatedObject")


class Precalculator:
    """Precalculate expensive-to-create objects in a thread pool.

    Given a creation thread pool with N threads, the class will try to ensure
    that N objects are available at all times.  Extra unused objects will
    destroyed at shutdown.

    Put another way, if creation latency is ``L`` and there are ``N`` threads,
    worst case mean latency of ``get()`` will be ``L/N`` assuming an ongoing
    stream of ``get()``.  Individual calls to ``get()`` will still have worst
    case of ``L``.
    """

    def __init__(self, constructor, destructor, create_pool, destroy_pool):
        """Use create() instead."""
        self._constructor = constructor
        self._destructor = destructor
        self._create_pool = create_pool
        self._destroy_pool = destroy_pool
        self._queue = Queue()

    @classmethod
    def create(
            cls,
            constructor: Callable[[], CreatedObject],
            destructor: Callable[[CreatedObject],None]=lambda x: None,
            num_create_threads: int=2,
            num_destroy_threads: int=1
    ):
        """Create a new Precalculator.

        :param constructor: Factory to create objects.  Will be run in thread
            pool.
        :param destructor: Callable that takes created object and destroy its.
            Will run in threadpool.
        :param num_create_threads: Number of threads in creation threadpool.
        :param num_destroy_threads: Number of threads in destruction threadpool.

        :return: A new running ``Precalculator``.
        """
        precalc = Precalculator(
            constructor, destructor, ThreadPool(num_create_threads),
            ThreadPool(num_destroy_threads)
        )
        for i in range(num_create_threads):
            precalc._precalc()
        return precalc

    def _precalc(self):
        """Queue a precalculation of another object."""
        self._create_pool.apply_async(self._constructor, callback=self._queue.put)

    def stop(self):
        """
        Shutdown all the thread pools, waiting until all relevant objects are
        destroyed.
        """
        self._create_pool.close()
        self._create_pool.join()
        while self._queue.qsize():
            self.destroy(self._queue.get())
        self._destroy_pool.close()
        self._destroy_pool.join()

    def get(self) -> CreatedObject:
        """Get a precalculated object.

        Will usually return quickly, so long as some precalculated objects are
        available, otherwise it might have to wait until an object is available.
        """
        self._precalc()
        return self._queue.get()

    def destroy(self, obj: CreatedObject):
        """Schedule an object to be destroyed in a thread pool.

        Returns immediately.
        """
        self._destroy_pool.apply_async(self._destructor, (obj,))
