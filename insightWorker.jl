module insightWorker
include("insights.jl")
using .Insights
using Redis
using JSON
using Logging

Logging.configure(filename="logfile.log")
Logging.configure(level=DEBUG)

conn = RedisConnection()
pubsub = RedisConnection()

function start(o)
  debug("starting")
  envelope = JSON.parse(o[2])

  try
    insights = Insights.harvestInsights(envelope["payload"])
    envelope["payload"] = insights
    produce(envelope)
  catch e
    err(e)
    envelope["payload"] = []
    envelope["error"] = e
    produce(envelope)
  end
  # produce(envelope)
end

function sink(p::Task)
  for s in p
    publish(pubsub, s["returnKey"], JSON.json(s))
    debug("published")
    debug(s["returnKey"])
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

end
