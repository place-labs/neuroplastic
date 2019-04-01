require "rethinkdb-orm"
require "../neuroplastic"

# Spec models
####################################################################################################

class Basic < RethinkORM::Base
  include Neuroplastic
  attribute name : String
end

class Goat < RethinkORM::Base
  include Neuroplastic
  attribute name : String
end

class Kid < RethinkORM::Base
  include Neuroplastic
  attribute age : Int32
  attribute hoof_treatment : String
  belongs_to Goat
end
