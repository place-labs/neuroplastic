require "./neuroplastic/*"

module Neuroplastic
  macro included
    macro finished
      __generate_accessor
    end
  end

  macro __generate_accessor
    {% doc_type = @type.name.stringify.split("::").last %}
    @@elastic : Neuroplastic::Elastic({{ @type }}) = Neuroplastic::Elastic({{ @type }}).new # (index: @@table_name, type: {{ doc_type }})

    # Exposes the Neuroplastic elastic client
    # TODO: When crystal allows generic classes in unions, make this a lazy instantiation
    def self.elastic
      @@elastic
    end
  end
end
