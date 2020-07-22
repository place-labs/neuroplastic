require "./neuroplastic/*"

module Neuroplastic
  macro included
  {% if @type.abstract? %}
    macro inherited
      macro finished
          __generate_accessor
      end
    end
    {% else %}
    macro finished
        __generate_accessor
    end
  {% end %}
  end

  private macro __generate_accessor
    class_getter(elastic) { Neuroplastic::Elastic({{ @type }}).new }
  end
end
