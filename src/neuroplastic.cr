require "./neuroplastic/elastic"

module Neuroplastic
  @@elastic : Neuroplastic::Elastic | Nil

  macro included
    macro finished
      __generate_accessor
      __define_find_all
    end
  end

  macro __generate_accessor
    # Exposes the Neuroplastic elastic client
    def self.elastic
      {% doc_type = @type.name.stringify.split("::").last %}
      @@elastic || Neuroplastic::Elastic.new(index: @@table_name, type: {{ doc_type }})
    end
  end

  # Would ideally factor this class out, however it requires access to Model query methods
  macro __define_find_all
    class Neuroplastic::Elastic
      # Reopen class and define the find_all method
      def find_all(*ids)
        {{ @type }}.find_all(*ids)
      end
    end

  end
end
