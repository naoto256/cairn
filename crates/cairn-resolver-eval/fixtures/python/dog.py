from animal import Animal
from util import helper as aliased_helper


class Dog(Animal):
    def speak(self) -> str:
        return "woof"

    def run(self) -> None:
        super().speak()
        self.speak()
        Dog.build()
        aliased_helper()
