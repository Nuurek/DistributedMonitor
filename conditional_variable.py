class ConditionalVariable:
    _monitor = None
    _name = None

    def wait(self):
        pass

    def signal(self):
        pass

    def _initialize(self, monitor, name):
        self._monitor = monitor
        self._name = name

    @property
    def name(self):
        return self._name
