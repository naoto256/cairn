require_relative '../lib/loud_dog'
require_relative '../lib/utils'
class Usage
  def run
    ld = LoudDog.new
    ld.bark
    Utils::String.shout("hi")
  end
  def dynamic_call(obj)
    obj.method_that_might_not_exist
  end
end
