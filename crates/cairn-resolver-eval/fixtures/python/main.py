from cat import Cat
from dog import Dog


def main() -> None:
    pets = [Dog(), Cat()]
    for pet in pets:
        print(f"{pet.name()}: {pet.speak()}")


if __name__ == "__main__":
    main()
