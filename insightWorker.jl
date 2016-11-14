include("insights.jl")
using .Insights
using Redis
using JSON

conn = RedisConnection()
pubsub = RedisConnection()

function start(o)
  println("starting")
  envelope = JSON.parse(o[2])

  println(typeof(envelope["payload"]))

  try
    Insights.harvestInsights(envelope["payload"])
  catch e
    println(e)
    envelope["error"] = e
    produce(envelope)
  end
  # produce(envelope)
end

function sink(p::Task)
  for s in p
    publish(pubsub, s["returnKey"], "[]")
    println("published")
  end
end

function startTask(o)
  @sync begin
    a = @async start(o)
    @async sink(a)
  end
end

function listen()
  while true
    o = brpop(conn, "queue", 60)
    if (typeof(o) != Void)
      startTask(o)
    end
  end
end

listen()
