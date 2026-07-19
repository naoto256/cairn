class Animal:
    @classmethod
    def build(cls):
        return cls()

    def speak(self) -> str:
        return "..."

    def name(self) -> str:
        return type(self).__name__
