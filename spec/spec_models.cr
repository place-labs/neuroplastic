require "rethinkdb-orm"

class Basic < RethinkORM::Base
  attribute name : String
end

class Parent < RethinkORM::Base
  attribute name : String
end

class Child < RethinkORM::Base
  attribute age : Int32
  belongs_to Parent
end
