class FatalError(Exception):
    """Custom exception which is used when the exeption should terminate the script

        (It would be better if a generic unexpected Exception could do this,
        however that would require a refactor)
    """
    pass
