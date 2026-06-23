import { Greeter, hello } from "./greeter";

export class Hello implements Greeter {
  greet(name: string): string {
    return hello(name);
  }
}
