require "habitat"
require "http"
require "elasticsearch-crystal/elasticsearch/api"

class Neuroplastic::Client
  forward_missing_to client

  # Settings for elastic client
  Habitat.create do
    setting host : String = ENV["ES_HOST"]? || "127.0.0.1"
    setting port : Int32 = ENV["ES_PORT"]?.try(&.to_i) || 9200
  end

  @@client : Elasticsearch::API::Client | Nil

  def self.client
    @@client ||= Elasticsearch::API::Client.new({
      :host => self.settings.host,
      :port => self.settings.port,
    })
  end
end
