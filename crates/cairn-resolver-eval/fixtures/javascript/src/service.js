import { IntermediateService } from './intermediate';

export class Service extends IntermediateService {
    static build() { return new Service(); }

    step() {
        super.step();
    }

    run() {
        Service.build();
        this.step();
    }
}
