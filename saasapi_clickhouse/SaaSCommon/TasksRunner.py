# -*- coding: utf-8 -*-
import time
import threading
import Queue


class TasksRunner(object):

    tq = Queue.Queue(maxsize = -1)

    def __init__(self):
        t = threading.Thread(target=self.run, args=())
        t.start()

    def put(self, task):
        self.__class__.tq.put(task)

    def run(self):

        while True:
            print u"等待任务数量： ", self.__class__.tq.qsize()
            try:
                _task = self.__class__.tq.get(block=True, timeout=10)
                _task.start()
                _task.join()
            except Queue.Empty:
                time.sleep(1)
            except Exception:
                import traceback
                print traceback.print_exc()
                time.sleep(3)


if __name__ == "__main__":
    tester = TasksRunner()
    time.sleep(3)
    tester.put("666")
    time.sleep(30)