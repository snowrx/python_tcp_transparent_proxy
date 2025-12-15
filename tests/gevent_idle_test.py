import gevent
import gevent.pool


def idle(priority: int = 0):
    gevent.idle(priority)
    print(f"run priority {priority}")


if __name__ == "__main__":
    g = gevent.pool.Group()
    g.spawn(idle, 4)
    g.spawn(idle, 3)
    g.spawn(idle, 2)
    g.spawn(idle, 1)
    g.spawn(idle, 0)
    g.join()
