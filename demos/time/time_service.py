import time
from rpyc_async import Service


class TimeService(Service):
    def exposed_get_utc(self):
        return time.time()

    def exposed_get_time(self):
        return time.ctime()
