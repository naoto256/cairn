import { Hello } from "./hello";
import { Shout } from "./shout";

export function main(): void {
  const greeters = [new Hello(), new Shout()];
  for (const g of greeters) {
    console.log(g.greet("world"));
  }
}
