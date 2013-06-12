require "amqp"

EventMachine.run do
  connection = AMQP.connect
  channel    = AMQP::Channel.new(connection)

  low_queue    = channel.queue("low")
  high_queue    = channel.queue("high")
  
  exchange = channel.direct("")

  replies_queue = channel.queue("", :exclusive => true, :auto_delete => true)
  replies_queue.subscribe do |metadata, payload|
    puts "Reply: #{payload.inspect}"
  end

  messages_sent = 0

  EventMachine.add_periodic_timer(1.0) do
    channel.default_exchange.publish("LOW",
                                     :routing_key => low_queue.name,
                                     :message_id  => Kernel.rand(10101010).to_s,
                                     :reply_to    => replies_queue.name)

    channel.default_exchange.publish("HIGH",
                                     :routing_key => high_queue.name,
                                     :message_id  => Kernel.rand(10101010).to_s,
                                     :reply_to    => replies_queue.name)

    EventMachine.stop if messages_sent > 10

    messages_sent += 1
  end


  Signal.trap("INT") { connection.close { EventMachine.stop } }
end