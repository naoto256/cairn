import { Greeter } from "./greeter";

export class Shout implements Greeter {
  greet(name: string): string {
    return `HELLO, ${name.toUpperCase()}!`;
  }
}
