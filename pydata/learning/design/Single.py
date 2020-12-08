class Singleton(object):
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, '_instance'):
            org = super(Singleton, cls)
            # python 3.3+, when override both __new__and __init__, then don't pass parameters here
            # cls._instance = org.__new__(cls, *args, **kargs)
            cls._instance = org.__new__(cls)
        return cls._instance


class SingletonAtt(object):
    shared_state = {}  # static/class attribute

    def __new__(cls, *args, **kwargs):
        ob = super().__new__(cls)
        ob.__dict__ = cls.shared_state
        return ob


def singleton_decorator(cls):
    instances_dict = {}

    def get_instance(*args, **kwargs):
        if cls not in instances_dict:
            instances_dict[cls] = cls(*args, **kwargs)
        return instances_dict[cls]

    return get_instance


def test_single():
    # using __new__, id is the same, but the attribute is not
    class singleSpam(Singleton):
        def __init__(self, name):
            self._name = name

        def __str__(self):
            return self._name

    s1 = singleSpam('cay')
    print(str(s1), id(s1))
    s2 = singleSpam('coco')
    print(str(s2), id(s2))

    # using shared state: why the both id and attr are different, but it is singleton?
    class singleSpam2Att(SingletonAtt):
        def __init__(self, name):
            self._name = name

        def __str__(self):
            return self._name

    s3 = singleSpam2Att('cay2')
    print(s3, id(s3))
    s4 = singleSpam2Att('coco2')
    print(s4, id(s4))
    if s3 is s4:
        print("s3 shares state with s4")

    # singleton using decorator, seems the only one with same id and attribute
    @singleton_decorator
    class singleSpamDecorator:
        def __init__(self, name):
            self._name = name

        def __str__(self):
            return self._name

    s5 = singleSpamDecorator('cay4')
    print(s5, id(s5))
    s6 = singleSpamDecorator('coco4')
    print(s6, id(s6))

    # Singleton using module/import
    from design.Singleton_module import singleton_module_instance
    print(singleton_module_instance, id(singleton_module_instance))


class strategy:
    def common_func(self):
        print("father")


if __name__ == '__main__':
    test_single()
