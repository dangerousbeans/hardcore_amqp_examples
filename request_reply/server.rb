require "amqp"
require 'pqueue'

EventMachine.run do
  connection = AMQP.connect
  channel    = AMQP::Channel.new(connection)

  to_process = PQueue.new {|a,b| a[0] > b[0] }
  mutex = Mutex.new

  low_requests_queue    = channel.queue("low")
  high_requests_queue    = channel.queue("high")

  low_requests_queue.subscribe(:ack => true) do |metadata, payload|
    puts "Got a request."
    
    mutex.synchronize { to_process << [0, metadata, payload] }
  end


  high_requests_queue.subscribe(:ack => true) do |metadata, payload|
    puts "Got a request."
    
    mutex.synchronize { to_process << [10, metadata, payload] }
  end


  Thread.new do
    loop do
      _, metadata, payload = mutex.synchronize { to_process.pop }

      if payload
        puts "#{payload}"
        process channel, metadata, payload
      else
        sleep(0.1)
      end
    end
  end

  Signal.trap("INT") { connection.close { EventMachine.stop } }


  def process channel, metadata, message
    channel.default_exchange.publish(message,
                                     :routing_key    => metadata.reply_to,
                                     :correlation_id => metadata.message_id,
                                     :mandatory      => true)

    EM.next_tick { metadata.ack }

    sleep(1)
  end
end