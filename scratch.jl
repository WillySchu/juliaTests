a = [1,2,3,4,5,6,7,8,9,0]

function go()
  local word = ""
  for i in a
    if word != ""
      println(word)
    end
    word = i
  end
end

go()
