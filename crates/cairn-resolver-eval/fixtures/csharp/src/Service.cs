using Api;
using Lib;
using static Lib.Helpers;

namespace App;

public class Service : IntermediateService, IGreeter {
    public static Service Build() { return new Service(); }
    public void Greet() {}

    public void Run() {
        this.Greet();
        base.Step();
        Service.Build();
        RunHelper();
    }
}
