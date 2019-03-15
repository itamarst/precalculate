"""Tests for precalculate.py."""

from threading import get_ident, enumerate as enumerate_threads
from time import sleep, time

from precalculate import Precalculator


class Obj:
    """An object that is expensive to create."""
    def __init__(self):
        self.destroyed = False
        self.create_thread = get_ident()

    def slow_destroy(self):
        sleep(0.1)
        self.destroy()

    def destroy(self):
        self.destroyed = True
        self.destroy_thread = get_ident()


class Factory:
    """A factory for objects."""
    def __init__(self):
        self.main_thread = get_ident()
        self.created = []

    def create(self):
        result = Obj()
        self.created.append(result)
        return result

    def slow_create(self):
        sleep(0.1)
        return self.create()


def test_created_in_threadpool():
    """Objects are created in a thread pool."""
    factory = Factory()
    precalc = Precalculator.create(factory.create)
    obj = precalc.get()
    assert isinstance(obj, Obj)
    assert obj.create_thread != factory.main_thread


def test_destroyed_in_threadpool():
    """Objects are destroyed in a thread pool."""
    factory = Factory()
    precalc = Precalculator.create(factory.create, Obj.destroy)
    obj = precalc.get()
    assert not obj.destroyed
    precalc.destroy(obj)
    sleep(0.1)
    assert obj.destroyed
    assert obj.destroy_thread != factory.main_thread


def test_precreated():
    """Objects are created in advance."""
    precalc = Precalculator.create(Factory().slow_create)
    sleep(0.5)  # more than enough time to precreate
    start = time()
    precalc.get()
    assert time() - start < 0.05


def test_new_create_on_get():
    """When an object is gotten, a new one is precreated in parallel."""
    precalc = Precalculator.create(Factory().slow_create)
    sleep(0.5)  # more than enough time to precreate
    assert precalc._queue.qsize() == 2
    precalc.get()
    sleep(0.2)
    assert precalc._queue.qsize() == 2


def test_destroy_is_async():
    """``destroy()`` doesn't wait for destruction."""
    factory = Factory()
    precalc = Precalculator.create(factory.create, Obj.slow_destroy)
    obj = precalc.get()
    start = time()
    precalc.destroy(obj)
    assert time() - start < 0.05


def test_stop():
    """``stop()`` destroys all precreated objects, and waits for destroys to finish."""
    previous_threads = set(enumerate_threads())
    factory = Factory()
    precalc = Precalculator.create(factory.create, Obj.slow_destroy)
    for _ in range(10):
        obj = precalc.get()
        precalc.destroy(obj)
    precalc.stop()
    assert len(factory.created) == 12
    assert set(o.destroyed for o in factory.created) == {True}
    assert previous_threads == set(enumerate_threads())
