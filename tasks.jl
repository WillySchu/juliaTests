println("Hello")

function source()
  println("source start")
  produce("start")
  produce("stop")
  println("source end")
end

function sink(p::Task)
  println("sink start")
  for s in p
    println(s)
  end
  println("sink end")
end

@sync begin
   a = @async source()
   @async sink(a)
 end

println("Goodbye")
