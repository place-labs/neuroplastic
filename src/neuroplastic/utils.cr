module Neuroplastic
  module Utils
    def self.document_name(klass : Class)
      klass.name.split("::").last
    end
  end
end
