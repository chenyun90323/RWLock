import threading
from enum import Enum

class Mode(Enum):
    Unset = -1
    Write = 0
    Read  = 1

class RWLock(object):
    def __init__(self, write_first = True):
        self.lock = threading.Lock()
        self.rcond = threading.Condition(self.lock)
        self.wcond = threading.Condition(self.lock)
        self.read_waiter = 0           # 等待获取读锁的线程数
        self.write_waiter = 0          # 等待获取写锁的线程数
        self.state = 0                 # 正数：表示正在读操作的线程数   负数：表示正在写操作的线程数（最多-1）
        self.owners = []               # 正在操作的线程id集合
        self.write_first = write_first # 默认写优先，False表示读优先
        self.mode = Mode.Unset         # 对上下文管理器with\as使用

    def write_acquire(self, blocking = True):
        # 获取写锁只有当锁没人占用，或者当前线程已经占用
        me = threading.get_ident()
        with self.lock:
            while not self._write_acquire(me):
                if not blocking:
                    return False
                self.write_waiter += 1
                self.wcond.wait()
                self.write_waiter -= 1
        return True

    def _write_acquire(self, me: int):
        # 获取写锁只有当锁没人占用，或者当前线程已经占用
        if self.state == 0 or (self.state < 0 and me in self.owners):
            self.state -= 1
            self.owners.append(me)
            return True
        if self.state > 0 and me in self.owners:
            raise RuntimeError('cannot recursively wrlock a rdlocked lock')
        return False

    def read_acquire(self, blocking = True):
        # 获取读锁只有当写锁没人占用，或者当前线程已经占用
        me = threading.get_ident()
        with self.lock:
            while not self._read_acquire(me):
                if not blocking:
                    return False
                self.read_waiter += 1
                self.rcond.wait()
                self.read_waiter -= 1
        return True

    def _read_acquire(self, me: int):
        if self.state < 0:
            # 如果锁被写锁占用
            return False

        if not self.write_waiter:
            ok = True
        else:
            ok = me in self.owners
        if ok or not self.write_first:
            self.state += 1
            self.owners.append(me)
            return True
        return False

    def release(self):
        me = threading.get_ident()
        with self.lock:
            try:
                self.owners.remove(me)
            except ValueError:
                raise RuntimeError('cannot release un-acquired lock')

            if self.state > 0:
                self.state -= 1
            else:
                self.state += 1
            if not self.state:
                if self.write_waiter and self.write_first:   # 如果有写操作在等待（默认写优先）
                    self.wcond.notify()
                elif self.read_waiter:
                    self.rcond.notify_all()
                elif self.write_waiter:
                    self.wcond.notify()

    read_release = release
    write_release = release

    def __enter__(self):
        if self.mode == Mode.Write:
            self.write_acquire()
        elif self.mode == Mode.Read:
            self.read_acquire()
        else:
            raise Exception(f'{self} 模式错误: {self.mode}')
        return self

    def __exit__(self, *exc_info):
        if self.mode == Mode.Write:
            self.write_release()
        elif self.mode == Mode.Read:
            self.read_release()
        else:
            raise Exception(f'{self} 模式错误: {self.mode}')

    def __call__(self, mode: Mode):
        self.mode = mode
        return self

    def __repr__(self):
        s = '<RWLock '
        s += 'read_waiter: ' + str(self.read_waiter) + ', '
        s += 'write_waiter: ' + str(self.write_waiter) + ', '
        s += 'state: ' + str(self.state) + ', '
        s += 'write_first: ' + str(self.write_first) + '>'
        return s