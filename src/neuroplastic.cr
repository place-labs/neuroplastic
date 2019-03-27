require "./neuroplastic/elastic"

module Neuroplastic
  @@elastic : Neuroplastic::Elastic | Nil

  macro included
    macro finished
      __generate_accessor
    end
  end

  macro __generate_accessor
    # Exposes the Neuroplastic elastic client
    def self.elastic
      @@elastic || Neuroplastic::Elastic.new(@@table_name)
    end
  end
end
