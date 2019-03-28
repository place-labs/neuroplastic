class Neuroplastic::Error < Exception
  getter message

  def initialize(@message : String? = "")
    super(message)
  end

  class ElasticQueryError < Error
  end
end
