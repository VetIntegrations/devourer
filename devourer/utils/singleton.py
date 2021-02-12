class Singleton(type):
    """
    Metaclass that restricts instantiation of your class to one object

    Usage:

        class YourClass(object, metaclass=Singleton):
    """

    def __init__(self, *args, **kwargs):
        self.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super().__call__(*args, **kwargs)

        return self.__instance
