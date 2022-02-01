import time


def elapsed(since, round_places=2):
    return round(time.time() - since, round_places)