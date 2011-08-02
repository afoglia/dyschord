# ReadWrite lock.  Taken from <http://bugs.python.org/issue8800>

from threading import *
from threading import _get_ident

def RWLock(*args, **kwargs):
    return _RWLock(*args, **kwargs)

class _RWLock(object):
    """
    A Reader-Writer lock.  Allows multiple readers at the same time but
    only one Writer (with recursion).
    Writers have priority over readers.
    A RWLock is reentrant in a limited fashion:  A thread holding the lock
    can always get another read lock.  And a thread holding a write lock can get another
    write lock.  But in general, a thread holding a read lock cannot recursively acquire a write lock.
    Of course, any recursive lock (rdlock,  or wrlock) must be mathced with an release.
    """
    def __init__(self):
        self.lock  = Lock()
        self.rcond = Condition(self.lock)
        self.wcond = Condition(self.lock)
        self.nr = self.nw = 0 #number of waiting threads
        self.state = 0 #positive is readercount, negative writer count
        self.owning = [] #threads will be few, so a list is not inefficient

    def wrlock(self, blocking=True):
        """
        Get a Write lock
        """
        me = _get_ident()
        with self.lock:
            while not self._wrlock(me):
                if not blocking:
                    return False
                self.nw += 1
                self.wcond.wait()
                self.nw -= 1
        return True

    def _wrlock(self, me):
        #we can only take the write lock if no one is there, or we already hold the lock
        if self.state == 0 or (self.state < 0 and me in self.owning):
            self.state -= 1
            self.owning.append(me)
            return True
        if self.state > 0 and me in self.owning:
            raise RuntimeError("cannot recursively wrlock a rdlocked lock")
        return False

    def rdlock(self, blocking=True):
        """
        Read lock the lock
        """
        me = _get_ident()
        with self.lock:
            while not self._rdlock(me):
                if not blocking:
                    return False
                #keep track of the number of readers waiting to limit
                #the number of notify_all() calls required.
                self.nr += 1
                self.rcond.wait()
                self.nr -= 1
        return True

    def _rdlock(self, me):
        if self.state < 0:
            #we are write locked. See if we reacquire
            return self._wrlock(me)

        if not self.nw:
            ok = True #no writers waiting for the lock
        else:
            #there is a writer waiting, but still, allow for recursion
            ok = me in self.owning

        if ok:
            self.state += 1
            self.owning.append(me)
            return True
        return False

    def unlock(self):
        """
        Release the lock
        """
        me = _get_ident()
        with self.lock:
            try:
                self.owning.remove(me)
            except ValueError:
                raise RuntimeError("cannot release un-acquired lock")

            if self.state > 0:
                self.state -= 1
            else:
                self.state += 1
            if not self.state:
                if self.nw:
                    self.wcond.notify()
                elif self.nr:
                    self.rcond.notify_all()

    #acquire/release api, for RLock compatibility
    acquire = wrlock
    release = unlock

    #context manager, gets a write lock
    def __enter__(self):
        self.wrlock()
    def __exit__(self, e, v, tb):
        self.unlock()

    #A separate ctxt mgr for read locking
    class _RdLockContext(object):
        def __init__(self, lock):
            self.lock = lock
        def __enter__(self):
            self.lock.rdlock()
        def __exit__(self, e, v, tb):
            self.lock.unlock()
    def rdlocked(self):
        return self._RdLockContext(self)
    def wrlocked(self):
        return self #for symmetry

    #interface for condition variable.  Must hold a Write lock
    def _is_owned(self):
        return self.state < 0 and _get_ident() in self.owning

    def _release_save(self):
        #in a write locked state, get the recursion level and free the lock
        with self.lock:
            r = self.owning
            self.owning = []
            self.state = 0
            if self.nw:
                self.wcond.notify()
            elif self.nr:
                self.rcond.notify_all()
            return r

    def _acquire_restore(self, x):
        #reclaim the lock at the old recursion level
        self.wrlock()
        with self.lock:
            self.owning = x
            self.state = -len(x)

