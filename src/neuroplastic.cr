require "./neuroplastic/*"

module Neuroplastic
  macro included
    macro finished
        __generate_accessor
    end
  end

  macro __generate_accessor
    @@elastic : Neuroplastic::Elastic({{ @type }}) = Neuroplastic::Elastic({{ @type }}).new
    # Exposes the Neuroplastic elastic client
    # TODO: When crystal allows generic classes in unions, make this a lazy instantiation
    def self.elastic
      @@elastic
    end
  end
end
