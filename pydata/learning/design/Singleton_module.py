class SingletonM(object):
    '''
    Singleton in Module
    '''

    def __str__(self):
        return self.__doc__



singleton_module_instance = SingletonM()