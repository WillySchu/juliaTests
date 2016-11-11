include("workerPurse.jl")
using Redis
using JSON

conn = RedisConnection()
pubsub = RedisConnection()

function start(o)
  println("starting")
  envelope = JSON.parse(o[2])

  produce(envelope)
end

function sink(p::Task)
  for s in p
    println(s["returnKey"])
    r = JSON.json(s)
    println(length(string(r)))
    println(r[1:100])
    println(string(r)[end-100:end])
    publish(pubsub, s["returnKey"], r)
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
