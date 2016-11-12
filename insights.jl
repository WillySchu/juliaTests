module Insights
  function harvestInsights(arr::Array{Any,1})
    for day = arr
      if (day["query"]["start-date"] != day["query"]["end-date"])
        error("Harvestor Received Non Exploded Data")
      end
      date = day["query"]["start-date"]
      keyls = keys(day)
      for i = 1:length(day["rows"]), j = 1:length(day["rows"][1])

      end
      println(length(day["rows"]))
      println(length(day["rows"][1]))
      println(keyls)
      for k = keyls
        if k != "rows"
          println(k)
          println(day[k])
          println(">>>>>>>>>>>>>>>>>>")
        end
      end
      break
    end
  end
end
