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
  attribute teeth : Int32
  attribute job : String
end

class Child::Kid < AbstractBase
  attribute age : Int32
  attribute hoof_treatment : String
  attribute visits : Array(String)
  belongs_to Goat
end
