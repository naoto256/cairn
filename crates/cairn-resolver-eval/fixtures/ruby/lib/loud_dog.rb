require_relative 'dog'
require_relative 'logging'
class LoudDog < Dog
  include Logging
  def bark
    log("woof")
    super
  end
end
