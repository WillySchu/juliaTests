include("workerPurse.jl")
using Redis
using JSON

conn = RedisConnection()

function start(o)
  println("starting")
  produce(o[2])
  println("produced")
end

function sink(p::Task)
  for s in p
    println(s.returnKey)
    publish(conn, s.returnKey, s)
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
