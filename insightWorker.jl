include("workerPurse.jl")
using Redis
using JSON

conn = RedisConnection()

function start(o)
  println("starting")
  envelope = JSON.parse(o[2])
  produce(envelope)
end

function sink(p::Task)
  for s in p
    println(s)
    i = 0
    while i < 10^8
      i += 1
    end
    println(s["returnKey"])
    publish(conn, s["returnKey"], JSON.json(s))
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
    o = brpop(conn, "queue", 1)
    if (typeof(o) != Void)
      startTask(o)
    end
  end
end

listen()
