require "rethinkdb-orm"
require "../src/neuroplastic"

# Spec models
####################################################################################################

abstract class AbstractBase < RethinkORM::Base
  include Neuroplastic
end

class Base < RethinkORM::Base
  include Neuroplastic
  attribute owns : String = "all your bases"
end

class Basic < AbstractBase
  attribute name : String
end

class Goat < AbstractBase
  attribute name : String
  attribute teeth : Int32 = 0
  attribute job : String = "being a goat"
end

class Child::Kid < AbstractBase
  attribute age : Int32 = 0
  attribute hoof_treatment : String = "oatmeal scrub"
  attribute visits : Array(String) = [] of String
  belongs_to Goat
end
