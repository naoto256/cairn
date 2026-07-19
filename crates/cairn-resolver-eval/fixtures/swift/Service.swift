class Service: IntermediateService, Greeter {
    static func build() -> Service { Service() }
    func greet() {}

    override func step() {
        super.step()
    }

    func run() {
        self.greet()
        Service.build()
    }
}
