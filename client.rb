require "rubygems"
require "amqp"

EventMachine.run do
  connection = AMQP.connect(:host => '127.0.0.1')
  channel  = AMQP::Channel.new(connection)
  low_queue    = channel.queue("low")
  high_queue    = channel.queue("high")
  exchange = channel.direct("")


  100.times do 
    exchange.publish "LOW", :routing_key => low_queue.name
  end

  # EventMachine.add_periodic_timer(0.0001) do
  100.times do 
    exchange.publish "HIGH", :routing_key => high_queue.name
  end
  # end
   
  # connection.close { EventMachine.stop } 
end