package app

import api.Greeter
import base.IntermediateService
import util.helper as aliasedHelper

class Service : IntermediateService(), Greeter {
    companion object {
        fun build(): Service = Service()
    }

    fun run() {
        this.greet()
        super.step()
        Service.Companion.build()
        aliasedHelper()
    }
}
