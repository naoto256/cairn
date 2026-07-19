<?php
namespace App;

use App\Contracts\Greeter;
use App\Traits\Logging;

class Service extends IntermediateService implements Greeter {
    use Logging;

    public static function build(): Service { return new Service(); }
    public function greet(): void {}
    public function run(): void {
        self::build();
        parent::step();
    }
}
