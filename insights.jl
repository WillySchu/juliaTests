module Insights

dateRegex = r".+?(?=T)"

macro swap(x,y)
  quote
    local tmp = $(esc(x))
    $(esc(x)) = $(esc(y))
    $(esc(y)) = tmp
   end
end

function harvestInsights(arr::Array{Any,1})
  println("Harvesting...")
  results = Dict{String, Array}()

  if length(arr) < 1
    error("No Data Provided")
  elseif length(arr) == 1
    days = splitByDate(arr[1])
    checkContiguousDates(days)
  elseif length(arr) == 2
    return [compareArbitrary(arr[2], arr[1])]
  else
    days = arr
    checkContiguousDates(days)
  end

  dateStr = match(dateRegex, arr[end]["query"]["start-date"])
  date = Date(dateStr.match)

  results["dayvsYesterday"] = dayvsYesterday(days)

  if Dates.dayofweek(date) + 7 < length(days)
    results["weekToDate"] = weekToDate(days)
  end

  if Dates.dayofmonth(date) + Dates.daysinmonth(date - Dates.Month(1)) < length(days)
    results["monthToDate"] = monthToDate(days)
  end

  if Dates.dayofquarter(date) + 92 < length(days)
    results["qtrToDate"] = qtrToDate(days)
  end

  if Dates.dayofyear(date) + 366 < length(days)
    results["yearToDate"] = yearToDate(days)
    results["dayvsLastYear"] = dayvsLastYear(days)
  end

  return results
end

function splitByDate(data::Dict{String, Any})::Array{Any, 1}
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

function checkContiguousDates(arr::Array{Any, 1})::Nothing
  local lastDate = ""
  oneDay = Dates.Day(1)
  for day in arr
    if lastDate == ""
      lastDate = Date(day["query"]["start-date"])
      continue
    end
    dif = Date(day["query"]["start-date"]) - lastDate
    if dif > oneDay
      error("data set not contiguous")
    end
    lastDate = Date(day["query"]["start-date"])
  end
end

function checkLeapDay(date1, date2)::Bool
  if date2 > date1
    @swap(date1, date2)
  end
  d1 = Dates.isleapyear(date1)
  d2 = Dates.isleapyear(date2)
  if d1
    return Date(Dates.year(date1), 29, 2) <= date1
  elseif d2
    return Date(Dates.year(date2), 29 ,2) >= date2
  else
    return false
  end
end

function sortByDate!(arr::Array{Any, 1})
  sort!(arr, by=x->x["query"]["start-date"])
end

function compareArbitrary(current::Array{Any, 1}, last::Array{Any, 1})::Array{Any, 1}
  current = aggregate(current)
  last = aggregate(last)
  dif = compare(current, last)
  return generateInsights(dif, 5)
end

function arbitraryPeriod(arr::Array{Any, 1}, len::Int64, offset::Int64)::Array{Any, 1}
  offset += len
  currentPeriod = arr[end-len+1:end]
  lastPeriod = arr[end-offset-len+1:end-offset]
  return compareArbitrary(currentPeriod, lastPeriod)
end

function weekToDate(arr::Array{Any, 1})::Array{Any, 1}
  println("Week to Date")
  startDate = Date(arr[end]["query"]["start-date"])
  dayOfWeek = Dates.dayofweek(startDate)
  currentPeriod = arr[end-dayOfWeek+1:end]
  lastPeriod = arr[end-7-dayOfWeek+1:end-7]
  return compareArbitrary(currentPeriod, lastPeriod)
end

function monthToDate(arr::Array{Any, 1})::Array{Any, 1}
  println("Month to Date")
  date = Date(arr[end]["query"]["start-date"])
  dayOfMonth = Dates.dayofmonth(date)
  daysInPrevMonth = Dates.daysinmonth(date - Dates.Month(1))
  currentPeriod = arr[end-dayOfMonth+1:end]
  lastPeriod = arr[end-dayOfMonth-daysInPrevMonth+1:end-daysInPrevMonth]
  return compareArbitrary(currentPeriod, lastPeriod)
end

function qtrToDate(arr::Array{Any, 1})::Array{Any, 1}
  println("Quarter to Date")
  quarters = [90, 91, 92, 92]
  date = Date(arr[end]["query"]["start-date"])
  if Dates.isleapyear(date)
    quarters[1] += 1
  end
  qtrOfYear = Dates.quarterofyear(date)
  lastQtr = qtrOfYear === 1 ? 4 : qtrOfYear - 1
  dayOfQtr = Dates.dayofquarter(date)
  currentPeriod = arr[end-dayOfQtr+1:end]
  lastPeriod = arr[end-quarters[lastQtr]-dayOfQtr:end-quarters[lastQtr]]
  return compareArbitrary(currentPeriod, lastPeriod)
end

# Fix to correctly get yearLength for date by checking on which side of
# the leap day it falls (if applicable)
function yearToDate(arr::Array{Any, 1})::Array{Any, 1}
  println("yearToDate")
  date = Date(arr[end]["query"]["start-date"])
  yearLength = checkLeapDay(date, date - Dates.Year(1)) ? 366 : 365
  dayOfYear = Dates.dayofyear(date)
  currentPeriod = arr[end-dayOfYear+1:end]
  lastPeriod = arr[end-yearLength-dayOfYear+1:end-yearLength]
  return compareArbitrary(currentPeriod, lastPeriod)
end

function dayvsYesterday(arr::Array{Any, 1})::Array{Any, 1}
  println("dayvsYesterday")
  return arbitraryPeriod(arr, 1, 0)
end

function dayvsLastYear(arr::Array{Any, 1})::Array{Any, 1}
  println("dayvsLastYear")
  yearLength = checkLeapDay(date, date - Dates.Year(1)) ? 366 : 365
  return arbitraryPeriod(arr, 1, yearLength)
end

# Probably can remove the following commented functions

# function dayByDay(arr::Array{Any, 1}, n::Int64)
#   println("Comparing by day...")
#   today = arr[end:end]
#   yesterday = arr[end-n:end-n]
#   return compareArbitrary(today, yesterday)
# end
#
# function weekByWeek(arr::Array{Any, 1}, w::Int64)
#   println("Comparing by week...")
#   n = 7 * w
#   thisWeek = arr[end-6:end]
#   lastWeek = arr[end-6-n:end-n]
#   return compareArbitrary(thisWeek, lastWeek)
# end
#
# function monthByMonth(arr::Array{Any, 1})
#   println("Comparing by month")
#   thisMonth = arr[end-29:end]
#   lastMonth = arr[end-59:end-30]
#   return compareArbitrary(thisMonth, lastMonth)
# end
#
# function yearByYear(arr::Array{Any, 1})
#   println("comparing by year...")
#   thisYear = arr[end-264:end]
#   lastYear = arr[end-719:end-365]
#   return compareArbitrary(thisYear, lastYear)
# end

function generateInsights(dif::Dict{String, Any}, n::Int64)::Array{Any, 1}
  insights = []
  meta = dif["meta"]
  for met in keys(dif)
    if met == "meta"
      continue
    end
    for dim in keys(dif[met])
      insight = Dict{String, Any}()
      insight["startDate"] = meta["startDate"]
      insight["endDate"] = meta["endDate"]
      insight["metric"] = met
      insight["dimensions"] = dim
      insight["type"] = "type"
      insight["percentChange"] = dif[met][dim]["score"]
      insight["significance"] = scoreSignificance(insight, met, dim, dif)
      # TODO: handle infinity better
      if insight["percentChange"] != Inf
        push!(insights, insight)
      end
    end
  end
  sort!(insights, by=x->x["significance"], rev=true)
  return insights[1:n]
end

function scoreSignificance(insight, met, dim, dif)::Float64
  mag = dif[met][dim]["magnitude"]
  insight["magnitude"] = mag
  insight["mag1"] = dif[met][dim]["mag1"]
  if haskey(dif[met][dim], "mag2")
    insight["mag2"] = dif[met][dim]["mag2"]
  end
  normMag = mag / dif["meta"]["largest"][met]["mag"]
  normPerc = insight["percentChange"] / dif["meta"]["largest"][met]["perc"]
  return normMag + normPerc
end

function compare(first::Dict{String, Any}, second::Dict{String, Any})::Dict{String, Any}
  inn = []
  out = []
  dif = Dict{String, Any}()
  largest = Dict{String, Any}()
  for met in keys(first)
    if met == "meta"
      continue
    end
    largest[met] = Dict{String, Float64}()
    largest[met]["mag"] = 0
    largest[met]["perc"] = 0

    if !haskey(dif, met)
      dif[met] = Dict{String, Any}()
    end
    for dim in keys(first[met])
      dif[met][dim] = Dict{String, Float64}()
      if haskey(second[met], dim)
        if largest[met]["mag"] < second[met][dim] + first[met][dim]
          largest[met]["mag"] = second[met][dim] + first[met][dim]
        end
        push!(inn, dim)
        if first[met][dim] == 0
          if second[met][dim] == 0
            dif[met][dim]["score"] = 0
          else
            dif[met][dim]["score"] = -1
          end
        else
          dif[met][dim]["score"] = (first[met][dim] - second[met][dim]) / second[met][dim]
        end
        dif[met][dim]["magnitude"] = first[met][dim] + second[met][dim]
        dif[met][dim]["mag1"] = first[met][dim]
        dif[met][dim]["mag2"] = second[met][dim]
      else
        if largest[met]["mag"] < first[met][dim]
          largest[met]["mag"] = first[met][dim]
        end
        dif[met][dim]["score"] = Inf
        dif[met][dim]["magnitude"] = first[met][dim]
        dif[met][dim]["mag1"] = first[met][dim]
        push!(out, dim)
      end
      if largest[met]["perc"] < dif[met][dim]["score"]
        largest[met]["perc"] = dif[met][dim]["score"] == Inf ?
          largest[met]["perc"] : dif[met][dim]["score"]
      end
    end
  end
  meta = Dict{String, Any}()
  meta["startDate"] = second["meta"]["startDate"]
  meta["endDate"] = first["meta"]["endDate"]
  meta["largest"] = largest
  dif["meta"] = meta
  return dif
end

function aggregate(arr::Array{Any, 1})::Dict{String, Any}
  agg = Dict{String, Any}()
  meta = Dict{String, Any}()
  agg["meta"] = meta
  dimensions = arr[1]["query"]["dimensions"]
  metrics = arr[1]["query"]["metrics"]
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
          if length(dimName) == 0
            dimName = string(dimName, day["rows"][i][j])
          else
            dimName = string(dimName, ',', day["rows"][i][j])
          end
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
