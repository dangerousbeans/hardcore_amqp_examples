require "amqp"

EventMachine.run do
  connection = AMQP.connect
  channel    = AMQP::Channel.new(connection)

  replies_queue = channel.queue("", :exclusive => true, :auto_delete => true)
  replies_queue.subscribe do |metadata, payload|
    puts "Reply: #{payload.inspect}"
  end

  # request time from a peer every 3 seconds
  EventMachine.add_periodic_timer(3.0) do
    puts "[request] Sending a request..."
    channel.default_exchange.publish("Hello?",
                                     :routing_key => "test.messages",
                                     :message_id  => Kernel.rand(10101010).to_s,
                                     :reply_to    => replies_queue.name)
  end


  Signal.trap("INT") { connection.close { EventMachine.stop } }
end