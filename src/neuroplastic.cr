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
    {% found = nil %}
    {% for par in @type.ancestors %}
      {% if tables = par.constant(:TABLES) %}
        {%  found = tables.uniq.size %}
      {% end %}
    {% end %}
    {% unless found %}
       {% raise "Cannot find constant TABLES in any of the parent class" %}
    {% end %}
    Neuroplastic::Client.indices = {{found}}
    class_getter(elastic) { Neuroplastic::Elastic({{ @type }}).new }
  end
end
