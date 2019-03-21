# neuroplastic

A module for [rethinkdb-orm](https://github.com/spider-gazelle/rethinkdb-orm).<br>
Exposes an elasticsearch query DSL that automagically resolves relations between the models.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     neuroplastic:
       github: aca-labs/neuroplastic
   ```

2. Run `shards install`

## Usage

This a sketch of the API. DO NOT USE AS REFERENCE!

```crystal
require "neuroplastic"
class Model < RethinkORM::Base
  include Neuroplastic::Elastic
  attribute name : String
  attribute age : Int32
end

# Rough api, most likely to be less of a dsl and rather a thin elasticsearch wrapper
query = Model.elastic()

# Construct query like so...
query.raw_filter({"name": "bill"})
query.name.sort.desc
query.age.filter(lte: 30)
query.name.search_field # weight name field

# Alternative
query.raw_filter({"name": "bill"})
query.sort["name"] = Neuroplastic::Sort::Desc
query.filter = {} # Some filter object
query.search_field("name") # Weight the field

# Dump the constructed query
puts query.to_json

# Perform search
query.search

# Or
Model.search query
```

## Contributing

1. [Fork it](<https://github.com/aca-labs/neuroplastic/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Caspian Baska](https://github.com/Caspiano) - creator and maintainer
