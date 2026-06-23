export interface Greeter {
  greet(name: string): string;
}

export function hello(name: string): string {
  return `hello, ${name}`;
}
