require "rubygems"
require "amqp"

EventMachine.run do
  connection = AMQP.connect(:host => '127.0.0.1')
  channel_low  = AMQP::Channel.new(connection)
  channel_high  = AMQP::Channel.new(connection)

  channel_low.prefetch(10)
  channel_high.prefetch(200)

  low_queue    = channel_low.queue("low", :auto_delete => false)
  high_queue    = channel_high.queue("high", :auto_delete => false)
  # exchange = channel.direct("")

  low_queue.subscribe do |payload|
    puts "#{payload}"
    slow_task
  end

  high_queue.subscribe do |payload|
    puts "#{payload}"
    slow_task
  end


  def slow_task
    sleep(1)
  end
end