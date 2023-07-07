# Yeah, hardcoding is ugh, but there doesn’t seem to be a portable way to determine this during
# runtime, and 2**31 throws an OverflowError on some architectures.
MAX_TIMESTAMP = 2**31 - 1
