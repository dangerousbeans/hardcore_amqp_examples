require "rubygems"
require "amqp"

EventMachine.run do
  connection = AMQP.connect(:host => '127.0.0.1')
  channel  = AMQP::Channel.new(connection)
  low_queue    = channel.queue("low")
  high_queue    = channel.queue("high")
  exchange = channel.direct("")


  10.times do |i| 
    message = "LOW #{i}"
    puts "sending: #{message}"
    exchange.publish message, :routing_key => low_queue.name
  end

  # EventMachine.add_periodic_timer(0.0001) do
  10.times do |i|
    message = "HIGH #{i}"
    puts "sending: #{message}"
    exchange.publish message, :routing_key => high_queue.name
  end
  # end
   
  # connection.close { EventMachine.stop } 
end