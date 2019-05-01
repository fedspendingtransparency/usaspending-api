def deprecated(func, *args, **kwargs):
    def wrapper_deprecated(*args, **kwargs):
        response = func(*args, **kwargs)
        response['Warning 299'] = "Deprecated API"
        # if response.status_code == 200:
        #     response.status_code = 299
        return response
    return wrapper_deprecated
