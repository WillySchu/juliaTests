module Insights

function harvestInsights(arr::Array{Any,1})
  println("Harvesting...")
  results = []

  days = splitByDate(arr[1])
  checkContiguousDates(days)
  if length(days) < 2
    error("Not enough data to process")
  end

  push!(results, dayByDay(days, 1))
  if length(days) < 14
    return results
  end

  push!(results, weekByWeek(days, 1))
  if length(days) < 60
    return results
  end

  # push!(results, weekByWeek(days))
  # if length(days) < 730
  #   return results
  # end

  # push!(results, yearByYear(days))

  return results
end

function splitByDate(data::Dict{String, Any})
  dateRegex = r".+?(?=T)"
  result = []
  splits = Dict{String, Any}()

  for row in data["rows"]
    date = match(dateRegex, row[1]).match
    if !haskey(splits, date)
      splits[date] = []
    end
    push!(splits[date], row[2:end])
  end

  for date in keys(splits)
    res = Dict{String, Any}()
    res["query"] = Dict{String, Any}()
    res["query"]["start-date"] = date
    res["query"]["end-date"] = date
    res["query"]["dimensions"] = data["query"]["dimensions"]
    res["query"]["metrics"] = data["query"]["metrics"]
    res["rows"] = splits[date]
    res["columnHeaders"] = data["columnHeaders"][2:end]
    push!(result, copy(res))
  end
  sortByDate!(result)
  return result
end

function checkContiguousDates(arr::Array{Any, 1})
  for day in arr
    if isdefined(:lastDate)
      println(lastDate - day["query"]["start-date"])
    end
    lastDate = day["query"]["start-date"]
  end
end

function sortByDate!(arr::Array{Any, 1})
  sort!(arr, by=x->x["query"]["start-date"])
end

function dayByDay(arr::Array{Any, 1}, n::Int64)
  println("Comparing by day...")
  today = aggregate(arr[end:end])
  yesterday = aggregate(arr[end-n:end-n])
  diff = compare(today, yesterday)
  insights = generateInsights(diff, 5)
  return insights
end

function weekByWeek(arr::Array{Any, 1}, w::Int64)
  println("Comparing by week...")
  n = 7 * w
  thisWeek = aggregate(arr[end-6:end])
  lastWeek = aggregate(arr[end-6-n:end-n])
  diff = compare(thisWeek, lastWeek)
  insights = generateInsights(diff, 5)
  return insights
end

function monthByMonth(arr::Array{Any, 1})
  println("Comparing by month")
  thisMonth = aggregate(arr[end-29:end])
  lastMonth = aggregate(arr[end-59:end-30])
  diff = compare(thisMonth, lastMonth)
  insights = generateInsights(diff, 5)
  return insights
end

function yearByYear(arr::Array{Any, 1})
  println("comparing by year...")
  thisYear = aggregate(arr[end-264:end])
  lastYear = aggregate(arr[end-719:end-365])
  diff = compare(thisYear, lastYear)
  insights = generateInsights(diff, 5)
  return insights
end

function generateInsights(diff::Dict{String, Any}, n::Int64)
  println("Generating...")
  insights = []
  meta = diff["meta"]
  for met in keys(diff)
    if met == "meta"
      continue
    end
    for dim in keys(diff[met])
      insight = Dict{String, Any}()
      insight["startDate"] = meta["startDate"]
      insight["endDate"] = meta["endDate"]
      insight["metrics"] = met
      insight["dimensions"] = dim
      insight["type"] = "type"
      insight["percentChange"] = diff[met][dim]["score"]
      mag = diff[met][dim]["magnitude"]
      insight["magnitude"] = mag
      insight["mag1"] = diff[met][dim]["mag1"]
      if haskey(diff[met][dim], "mag2")
        insight["mag2"] = diff[met][dim]["mag2"]
      end
      norm = mag / diff["meta"]["largest"][met] + 1
      insight["significance"] = norm + abs(insight["percentChange"])
      # TODO: handle infinity better
      if insight["percentChange"] != Inf
        push!(insights, insight)
      end
    end
  end
  sort!(insights, by=x->x["significance"], rev=true)
  return insights[1:n]
end

function compare(first::Dict{String, Any}, second::Dict{String, Any})
  println("Comparing...")
  inn = []
  out = []
  diff = Dict{String, Any}()
  largest = Dict{String, Float64}()
  for met in keys(first)
    if met == "meta"
      continue
    end
    largest[met] = 0

    if !haskey(diff, met)
      diff[met] = Dict{String, Any}()
    end
    for dim in keys(first[met])
      diff[met][dim] = Dict{String, Float64}()
      if haskey(second[met], dim)
        if largest[met] < second[met][dim] + first[met][dim]
          largest[met] = second[met][dim] + first[met][dim]
        end
        push!(inn, dim)
        if first[met][dim] == 0
          if second[met][dim] == 0
            diff[met][dim]["score"] = 0
          else
            diff[met][dim]["score"] = -1
          end
        else
          diff[met][dim]["score"] = (first[met][dim] - second[met][dim]) / second[met][dim]
        end
        diff[met][dim]["magnitude"] = first[met][dim] + second[met][dim]
        diff[met][dim]["mag1"] = first[met][dim]
        diff[met][dim]["mag2"] = second[met][dim]
      else
        if largest[met] < first[met][dim]
          largest[met] = first[met][dim]
        end
        diff[met][dim]["score"] = Inf
        diff[met][dim]["magnitude"] = first[met][dim]
        diff[met][dim]["mag1"] = first[met][dim]
        push!(out, dim)
      end
    end
  end
  meta = Dict{String, Any}()
  meta["startDate"] = second["meta"]["startDate"]
  meta["endDate"] = first["meta"]["endDate"]
  meta["largest"] = largest
  diff["meta"] = meta
  return diff
end

function aggregate(arr::Array{Any, 1})
  agg = Dict{String, Any}()
  meta = Dict{String, Any}()
  agg["meta"] = meta
  dimensions = arr[1]["query"]["dimensions"]
  metrics = arr[1]["query"]["metrics"]
  dateRegex = r".+?(?=T)"
  for met in metrics
    agg[met] = Dict()
  end
  x = 0
  for day in arr
    startDate = Date(day["query"]["start-date"])
    endDate = Date(day["query"]["end-date"])

    if startDate != endDate
      error("Harvestor received non exploded data")
    end

    if !haskey(meta, "startDate")
      meta["startDate"] = startDate
    end
    if !haskey(meta, "endDate")
      meta["endDate"] = endDate
    end

    if startDate < meta["startDate"]
      meta["startDate"] = startDate
    end

    if endDate > meta["endDate"]
      meta["endDate"] = endDate
    end

    for i = 1:length(day["rows"])
      dimName = ""
      for j = 1:length(day["rows"][1])
        if day["columnHeaders"][j]["columnType"] == "DIMENSION"
          dimName = string(dimName, day["rows"][i][j])
        else
          met = day["columnHeaders"][j]["name"]
          if haskey(agg[met], dimName)
            agg[met][dimName] += parse(Float64, day["rows"][i][j])
          else
            agg[met][dimName] = parse(Float64, day["rows"][i][j])
          end
        end
      end
    end
  end
  return agg
end
end
