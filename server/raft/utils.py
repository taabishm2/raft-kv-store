def debug(func):
    def wrapper(*args, **kwargs):
        print(f"[LOG] >>> Calling {func.__name__} with args={args} kwargs={kwargs}")
        result = func(*args, **kwargs)
        print(f"[LOG] <<< {func.__name__} returned {result}")
        return result

    return wrapper
