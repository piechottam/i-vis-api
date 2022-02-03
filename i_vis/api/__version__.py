"""Version information."""

__MAJOR__ = 0
__MINOR__ = 9
__PATCH__ = 1
__SUFFIX__ = "dev"

__VERSION__ = ".".join(map(str, (__MAJOR__, __MINOR__, __PATCH__))) + (
    "-" + __SUFFIX__ if __SUFFIX__ else ""
)
